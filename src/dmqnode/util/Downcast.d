/*******************************************************************************

    Object downcast asserting the result is in a sane state.

    copyright:
        Copyright (c) 2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.util.Downcast;

/*******************************************************************************

    Casts the class object `from` to `To`, asserting that the cast was
    successful (i.e. the result is not `null`) and calling the object invariant.
    Using this function is safer than a plain cast, as it also ensures at
    compile-time that `From` is a class or an interface.

    Functionality is the same as `downcast` in `ocean.core.TypeConvert` with
    the extensions of
        1. `assert(to)` (where `to` is the returned object),
        2. allowing interfaces for `To`.

    Params:
        from = object to be cast to type `To`

    Returns:
        `cast(To)from`, which is asserted to be not `null`.

*******************************************************************************/

public To downcastAssert ( To, From ) ( From from )
out (to)
{
    assert(to, "Unable to cast from \"" ~ From.stringof ~ ":" ~
           (cast(Object)from).classinfo.name ~ "\" to " ~ To.stringof);
}
do
{
    static assert(is(To == class) || is(To == interface));
    static assert(is(From == class) || is(From == interface));
    return cast(To)(from);
}
