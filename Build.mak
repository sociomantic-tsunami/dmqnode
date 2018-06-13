ifeq ($(DVER),1)
	override DFLAGS += -v2 -v2=-static-arr-params -v2=-volatile
else
	DC = dmd-transitional
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

allunittest: override DFLAGS += -debug=OvfMinimizeTest -debug=Full
allunittest: override LDFLAGS += -lpcre

# Additional flags needed when unittesting
#$O/%unittests: override LDFLAGS += 

# Package dependencies
$O/pkg-dmqnode.stamp: $B/dmqnode README.rst

$O/pkg-dmqnode-common.stamp: \
       $(PKG)/defaults.py README.rst deploy/upstart/dmq.conf
