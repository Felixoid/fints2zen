#!/usr/bin/env python


#  from argparse import ArgumentParser
import os
from argparse import ArgumentParser, Namespace, ArgumentDefaultsHelpFormatter
from cerberus import Validator
from datetime import datetime
from fints.client import FinTS3PinTanClient
from pprint import pprint
from re import search
from sys import exit, stderr
from uuid import uuid4
from yaml import safe_load, dump
from yaml.scanner import ScannerError
from zenmoney import (
    Diff,
    OAuth2,
    Request,
    Transaction,
    ZenObjectsList,
    timestamp,
    ZenMoneyException,
)

"""
First connection:
    * Get credentials for bank fin-ts (HBCI) and account info
"""


class InterTransaction(tuple):
    """
    Helping class to sorting transactions by first element
    """

    def __lt__(self, other):
        return self[0] < other[0]


class InterTransactions(list):
    """
    Intermidiate class to compare transaction from zenmoney and fints
    """

    def keys(self):
        return [i[0] for i in self]

    def compare_to(self, obj):
        s_only = self[:]
        o_only = obj[:]
        both = []
        for s in s_only[:]:
            try:
                index = [o[0] for o in o_only].index(s[0])
                both.append(((s[0]), ()))
                del o_only[index]
                s_only.remove(s)
            except ValueError:
                continue
        s_only.sort()
        o_only.sort()
        both.sort()
        return (
            InterTransactions(s_only),
            InterTransactions(both),
            InterTransactions(o_only),
        )


class FinTs(object):
    """
    Class for data from FinTS banking API
    """

    date_patterns = (
        # 2019-02-12T17:05:47 \/
        (r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d", "%Y-%m-%dT%H:%M:%S"),
        (r"\d{4}\.\d\d\.\d\dT\d\d\.\d\d\.\d\d", "%Y.%m.%dT%H.%M.%S"),
        (r"\d\d\.\d\d \d{6}", "%d.%m %H%M%S"),  # 14.02 163532 : 14.02163532ARN
        (r"\d\d\.\d{8}ARN", "%d.%m%H%M%SARN"),  # 21.02164412ARN
        (r"\d\d\.\d\d \d\d\.\d\d", "%d.%m %H.%M"),  # 12.02 09.56
        # 08.01 09:00 : 08.0109.00
        (r" \d\d\.\d\d \d\d:\d\d ", " %d.%m %H:%M "),
        (r" \d\d\.\d{4}\.\d\d ", " %d.%m%H.%M "),  # 08.0109.00
        (r"\d\d\.\d\d\.\d{4}", "%d.%m.%Y"),  # 01.02.2019
    )

    transaction_types = (  # Type by posting_text
        ("Entgelt", "Fee"),
        ("Gehalt/Rente", "Salary/Pension"),
        ("Abbuchung", "Debit entry"),
        ("Gutschrift", "Credit/Top up"),
        ("Überweisung", "Transfer"),
    )

    def __init__(self, blz: str, username: str, pin: str, url: str, *args, **kwargs):
        _fints = FinTS3PinTanClient(
            str(blz),
            str(username),
            str(pin),
            str(url),
            *args,
            product_id="9FA6681DEC0CF3046BFC2F8A6",
            **kwargs,
        )
        self.accounts = _fints.get_sepa_accounts()
        self.balance = {}
        self.transactions = {}
        for a in self.accounts:
            self.balance[a.iban] = _fints.get_balance(a)
            self.transactions[a.iban] = _fints.get_transactions(a)

    def _get_date(self, string, booking_date):
        """
        Accepts array from transaction info, returns the date
        First tries to get data from regexp+string format
        Then removes spaces from info string and tries to get isoformat
        Then returns date 'Buchung' (booking)
        """
        date = None
        for pattern in FinTs.date_patterns:
            s = search(pattern[0], string)
            if s:
                try:
                    date_str = s[0]
                    date = datetime.strptime(date_str, pattern[1]).date()
                except ValueError:
                    try:
                        # Trying to mitigate bools-t like 08.12360904ARN
                        date_str = date_str[:5]
                        date = datetime.strptime(date_str, "%d.%m").date()
                    except ValueError:
                        # So, here's another bools-t.
                        # 29.02 is out of range, so the year must be passed
                        date_str = f"{date_str}.{datetime.now().year}"
                        date = datetime.strptime(date_str, "%d.%m.%Y").date()

        if date is None:
            date = booking_date
        if date.year == 1900:
            if booking_date.month == 1 and date.month == 12:
                date = date.replace(year=booking_date.year - 1)
            else:
                date = date.replace(year=booking_date.year)
        return str(date)

    def get_transactions(self, iban: str, account_uuid: str, transfer_uuid: str):
        """
        iban: the IBAN number in account
        account_uuid: account ID in zenmoney for the IBAN
        transfer_uuid: zenmoney account UUID for default withdraw
        """
        transactions = InterTransactions()
        for tr in self.transactions[iban]:
            tr = tr.data
            date = self._get_date(str(tr["purpose"]), tr["date"])
            amount = float(tr["amount"].amount)

            # payee could be None
            payee = tr["applicant_name"] or ""
            if 0 < amount:
                income_amount = amount
                outcome_amount = 0.0
                income_account = account_uuid
                outcome_account = account_uuid
            elif amount < 0:
                income_amount = 0.0
                outcome_amount = -amount
                outcome_account = account_uuid
                # IngDiba related
                if payee.startswith("Bargeldauszahlung "):
                    # If payee starts with the word, this is a withdraw
                    payee = payee[18:]
                    income_account = transfer_uuid
                    income_amount = outcome_amount
                else:
                    income_account = account_uuid
            else:
                # ignore service messages with amount == 0
                continue

            # IngDiba related
            if payee.startswith("VISA "):
                payee = payee[5:]

            currency = tr["currency"]

            comment = tr["purpose"]
            transactions.append(
                InterTransaction(
                    (
                        (date, amount, currency),
                        {
                            "date": date,
                            "income": income_amount,
                            "incomeAccount": income_account,
                            "incomeInstrument": 3,
                            "outcome": outcome_amount,
                            "outcomeAccount": outcome_account,
                            "outcomeInstrument": 3,
                            "originalPayee": payee.strip(),
                            "payee": payee.strip(),
                            "comment": comment,
                            "id": str(uuid4()),
                            # '__raw': tr,
                        },
                    )
                )
            )
        return transactions


class Zen(object):
    """
    Class for data from ZenMoney API
    """

    def __init__(self, token: str, serverTimestamp: int):
        self.api = Request(token)
        self.diff = self.api.diff(
            Diff(
                serverTimestamp=serverTimestamp,
                forceFetch=[
                    "instrument",
                    "user",
                ],
            )
        )

    def get_transactions(self, account_uuid: str):
        def get_amount(tr: Transaction):
            if tr.incomeAccount == tr.outcomeAccount == account_uuid:
                # Normal transaction
                amount = max(tr.income, tr.outcome)
                if 0 < tr.outcome:
                    amount = -amount
                return amount, "income"
            elif tr.incomeAccount == account_uuid:
                return tr.income, "income"
            elif tr.outcomeAccount == account_uuid:
                return -tr.outcome, "outcome"
            else:
                raise Exception("Something is definitely broken")

        transactions = InterTransactions()
        for tr in self.diff.transaction.by_account(account_uuid):
            if tr.deleted:
                continue
            amount, tr_type = get_amount(tr)

            tr_currency_id = getattr(tr, tr_type + "Instrument")
            currency = self.diff.instrument.by_id(tr_currency_id).shortTitle
            transactions.append(
                InterTransaction(
                    (
                        (str(tr.date), amount, currency),
                        {
                            "date": tr.date,
                            "income": tr.income,
                            "incomeAccount": tr.incomeAccount,
                            "incomeInstrument": tr.incomeInstrument,
                            "outcome": tr.outcome,
                            "outcomeAccount": tr.outcomeAccount,
                            "outcomeInstrument": tr.outcomeInstrument,
                            "originalPayee": tr.originalPayee,
                            "comment": tr.comment,
                            "id": tr.id,
                        },
                    )
                )
            )

        return transactions


v = Validator(
    {
        "bank": {
            "type": "dict",
            "required": True,
            "schema": {
                "blz": {"required": True, "type": "integer"},
                "username": {
                    "required": True,
                    "anyof_type": ["string", "integer"],
                },
                "pin": {"required": True, "type": "string"},
                "url": {"required": True, "type": "string"},
            },
        },
        "zenmoney": {
            "required": True,
            "type": "dict",
            "schema": {
                "token": {"required": True, "type": "string"},
                "withdraw_account": {"required": True, "type": "string"},
            },
        },
        "accounts": {
            "required": True,
            "type": "list",
            "schema": {
                "type": "list",
                "minlength": 2,
                "maxlength": 2,
                "schema": {
                    "type": "string",
                    "regex": "^[-a-zA-Z0-9]+$",
                },
            },
            "minlength": 1,
        },
    }
)


def get_config(filename: str) -> dict:
    """
    Function to get or fill the config with the user interactive values

    Keyword argument:
    filename -- string with filename to read or write
    """
    try:
        with open(filename) as c:
            config = safe_load(c)
    except FileNotFoundError:
        return write_config(filename)
    except ScannerError:
        stderr.write("Fail to read yaml config fom file {}".format(filename))
        raise
    is_valid = v.validate(config)
    if not is_valid:
        stderr.write("Fail to validate the config: {}".format(v.errors))
        exit(1)
    return config


def write_config(filename: str) -> dict:
    """
    Fill the config with user input
    """
    config = {"bank": {}, "zenmoney": {}, "accounts": []}  # type: dict
    print("Filling up the config from the input.\nPlease, enter your bank credentials")
    config["bank"]["blz"] = int(input("  Please, enter blz (int): "))
    config["bank"]["username"] = str(input("  Please, enter username: "))
    config["bank"]["pin"] = str(input("  Please, enter pin: "))
    config["bank"]["url"] = str(input("  Please, enter url: "))
    print("Checking finTS credentials")
    bank = FinTs(
        config["bank"]["blz"],
        config["bank"]["username"],
        config["bank"]["pin"],
        config["bank"]["url"],
    )
    ibans = [a.iban for a in bank.accounts]

    print("Do you have a zenmoney oauth2 token?")
    if bool(int(input("1=Yes, 0=No [_1_/0]: ") or 1)):
        config["zenmoney"]["token"] = str(input("  Please, enter token: "))
    else:
        print(
            "You should register your application for zenmoney API,"
            "visit the page http://api.zenmoney.ru/consumer.html\n\n"
            "Please, enter necessary information to generate user token"
        )
        key = str(input("  Please, enter consumer_key: "))
        secret = str(input("  Please, enter consumer_secret: "))
        username = str(input("  Please, enter zenmoney username: "))
        password = str(input("  Please, enter zenmoney password: "))
        oauth = OAuth2(key, secret, username, password)
        config["zenmoney"]["token"] = oauth.token

    print("Checking ZenMoney credentials")
    zen = Zen(config["zenmoney"]["token"], 1)
    a_titles = [a.title for a in zen.diff.account]
    a_ids = [a.id for a in zen.diff.account]

    print("Next IBANs are available:\n  {}".format("\n  ".join(ibans)))
    print(
        "Next ZenMoney accounts are available:\n{}".format(
            "\n".join(["  {}: {}".format(a[0], a[1]) for a in zip(a_titles, a_ids)])
        )
    )

    config["zenmoney"]["withdraw_account"] = str(
        input("Enter the zenmoney account UUID for default withdraw transactions: ")
    )

    print('Enter space separated pairs of "IBAN" "zenmoney_UUID" to sync')
    while True:
        pair = [str(x) for x in input("empty line to stop: ").split(maxsplit=1)]
        if not pair:
            break

        if pair[0] not in ibans:
            print("{} not belongs to {}\nTry again".format(pair[0], ibans))
            continue
        if pair[1] not in a_ids:
            print("{} not belongs to {}\nTry again".format(pair[1], a_ids))
            continue
        config["accounts"].append(pair)

    is_valid = v.validate(config)
    if not is_valid:
        stderr.write("Fail to validate the config: {}".format(v.errors))
        exit(1)

    with open(filename, "w") as c:
        c.write(dump(config))
        print("Configuration is successfully written to {}".format(c.name))

    return config


def parse_args() -> Namespace:
    default_config = os.path.join(
        os.environ.get("APPDATA")
        or os.environ.get("XDG_CONFIG_HOME")
        or os.path.join(os.path.expandvars("$HOME"), ".config"),
        "fints2zen.yaml",
    )
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "-c",
        "--config",
        default=default_config,
        type=str,
        help="application config, will be filled if file does not exist",
    )
    parser.add_argument(
        "-m",
        "--mode",
        default="bulk",
        choices=["bulk", "serial", "dry-run"],
        help='"bulk" - send all transactions without approval,'
        ' "serial" - approve each transaction,'
        ' "dry-run" - only prints transactions to sync',
    )
    return parser.parse_args()


def bulk_send(zen: Zen, transactions: list) -> None:
    diff = Diff(serverTimestamp=timestamp(), transaction=transactions)
    zen.api.diff(diff)


def serial_send(zen: Zen, transactions: list) -> None:
    for tr in transactions:
        if bool(int(input("1=Yes, 0=No [_1_/0]: ") or 1)):
            diff = Diff(serverTimestamp=timestamp(), transaction=[tr])
            zen.api.diff(diff)


def main():
    args = parse_args()
    config = get_config(args.config)
    zen = Zen(config["zenmoney"]["token"], 1)
    bank = FinTs(
        config["bank"]["blz"],
        config["bank"]["username"],
        config["bank"]["pin"],
        config["bank"]["url"],
    )
    for pair in range(len(config["accounts"])):
        z_transactions = zen.get_transactions(config["accounts"][pair][1])
        b_transactions = bank.get_transactions(
            config["accounts"][pair][0],
            config["accounts"][pair][1],
            config["zenmoney"]["withdraw_account"],
        )
        (only_zen, both, only_bank) = z_transactions.compare_to(b_transactions)
        print(
            'Pair "{} to {}".\n'
            "Amount of transactions only in bank: {}\n"
            "Amount of transactions only in zenmoney: {}\n"
            "Already synced: {}".format(
                config["accounts"][pair][0],
                config["accounts"][pair][1],
                len(only_bank),
                len(only_zen),
                len(both),
            )
        )
        if not only_bank:
            continue
        bank_to_zen = [Transaction(user=zen.diff.user[0].id, **i[1]) for i in only_bank]
        try:
            suggest = zen.api.suggest(ZenObjectsList(bank_to_zen))
        except ZenMoneyException as e:
            print(e.response.__dict__)
            raise

        print("Transactions to sync: ")
        pprint(suggest)
        if args.mode == "bulk":
            bulk_send(zen, suggest)

        if args.mode == "serial":
            serial_send(zen, suggest)


if __name__ == "__main__":
    main()
