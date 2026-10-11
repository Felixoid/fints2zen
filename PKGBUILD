# Maintainer: Mikhail f. Shiryaev <mr dot felixoid at gmail dot com>

_name=python-fints2zen
pkgname="$_name-git"
pkgver=0.r11.d83023f
pkgrel=1
pkgdesc="Use the full Github API v3"
arch=('any')
license=('LGPL')
url="https://github.com/Felixoid/fints2zen"
provides=('python-fints2zen')
conflicts=('python-fints2zen')
depends=('python-zenmoney' 'python-fints' 'python-cerberus' 'python-yaml')
#makedepends=('python-setuptools-scm' 'python-build' 'python-installer')
#checkdepends=('python-pytest' 'python-cryptography' 'python-httpretty' 'python-parameterized' 'python-responses')
source=(fints2zen.py)
sha512sums=(SKIP)

pkgver() {
  printf "%s.r%s.%s" 0 "$(git rev-list --count HEAD)" "$(git rev-parse --short HEAD)"
}

package() {
  install -d "$pkgdir/usr/bin/"
  install -m0755 "$srcdir/fints2zen.py" "$pkgdir/usr/bin/fints2zen"
}
