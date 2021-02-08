MAX_LONG = 9223372036854775807
MIN_LONG = -9223372036854775808

MAX_INT = 2147483647
MIN_INT = -2147483648

NAN = float('nan')


def jtype_or(t, v, def_v):
    return t(def_v) if v is None else t(v)


def jfloat_or(v, def_v=NAN):
    return jtype_or(float, v, def_v)


def jlong_or(v, def_v):
    return jtype_or(int, v, def_v)
