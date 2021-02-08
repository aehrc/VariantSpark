import inspect


def merge_inits(*inits):
    def call_inits(self, *args, **kwargs):
        for init in inits:
            init(self, *args, **kwargs)

    return call_inits


def extend_cls(cls, mixin):
    for name, method in inspect.getmembers(mixin, predicate=inspect.ismethod):
        if name == '__init__':
            setattr(cls, name, merge_inits(cls.__init__.im_func, method.im_func))
        else:
            setattr(cls, name, method.im_func)
