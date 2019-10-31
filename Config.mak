DC := dmd-transitional
DVER := 2

# Ubuntu bionic requires builds to use position independent code and
# dmd-transitional does not set the flag -fPIC by default
ifeq ($(DC),dmd-transitional)
override DFLAGS += -fPIC
endif
