# check whether to build with position-independent code
# (may be required to build on newer distro releases)
ifeq ($(USE_PIC), 1)
	override DFLAGS += -fPIC
endif

ifeq ($F, production)
	override DFLAGS += -release
endif

override LDFLAGS += -llzo2 -lebtree -lrt -lgcrypt -lgpg-error -lglib-2.0

# Modules to exclude from testing
TEST_FILTER_OUT += \
	$T/src/dmqnode/main.d

$B/dmqnode: src/dmqnode/main.d
dmqnode: $B/dmqnode
all += dmqnode

$B/dmqperformance: src/dmqperformance/main.d
dmqperformance: $B/dmqperformance
all += dmqperformance

$O/test-dmqtest: dmqnode
$O/test-dmqtest: override LDFLAGS += -lpcre

$O/test-ovfminimize: override DFLAGS += -debug=OvfMinimizeTest -debug=Full

$O/test-loadfiles: dmqnode
$O/test-loadfiles: override LDFLAGS += -lpcre

$O/%unittests: override DFLAGS += -debug=OvfMinimizeTest -debug=Full
$O/%unittests: override LDFLAGS += -lpcre

# Additional flags needed when unittesting
#$O/%unittests: override LDFLAGS +=

# Package dependencies
$O/pkg-dmqnode.stamp: $B/dmqnode README.rst \
	$(PKG)/after_dmqnode_install.sh

$O/pkg-dmqnode-common.stamp: \
       $(PKG)/defaults.py README.rst
