"""
This class is taken from https://github.com/sebkeim/inner-class at commit sha
0856be1feee38710005a7ef27ae998af95dedbf8.
"""
from functools import update_wrapper


# TODO: re-arrange to another module
def raw_inner(x):
    """do nothing decorator for future backward compatibility :
    this will preserve current behavior for inner-class if a future version
    of the language change the default semantic for inner classes"""
    return x


class static_inner:
    """decorator for outer attribute"""

    def __init__(self, cls):
        self.icls = cls
        self.__doc__ = cls.__doc__

    def __set_name__(self, owner, name):
        self.icls.owner = owner
        # now that outer is set, replace decorator by the actual class
        setattr(owner, name, self.icls)


class class_inner(static_inner):
    """decorator for outer attribute, inner derivation and carried inheritance"""

    def __init__(self, cls):
        for method in ("__get__", "__set__", "__del__"):
            if hasattr(cls, method):
                raise ValueError("descriptors can't be used as inner class")
        static_inner.__init__(self, cls)

    def _innerparents(self, outercls):
        mro = self.icls.mro()
        name = self.name
        innerparents = []
        for parent in outercls.__bases__:
            try:
                innerparent = getattr(parent, name)
            except AttributeError:
                pass
            else:
                if innerparent not in mro:
                    innerparents.append(innerparent)
        return tuple(innerparents)

    def __set_name__(self, owner, name):
        # inner derivation
        self.name = name

        bases = self._innerparents(owner)
        if bases:
            selfbases = self.icls.__bases__
            if selfbases != (object,):
                bases = selfbases + bases
            self.icls = type(self.icls)(
                self.icls.__name__,
                bases,
                dict(self.icls.__dict__),
            )
        assert "outer" not in self.icls.__dict__
        self.icls.owner = owner

    def __get__(self, outerobj, outercls):
        # carried ineritence
        cls = self.icls
        if cls.owner != outercls:
            assert self.name not in outercls.__dict__

            bases = (self.icls,) + self._innerparents(outercls)
            cls = type(cls)(
                self.name,
                bases,
                {
                    "owner": outercls,
                    "__qualname__": outercls.__name__ + "." + self.name,
                    "__module__": cls.__module__,
                    "__doc__": cls.__doc__,
                    # '__annotations__':cls.__annotations__
                },
            )

            inner = type(self)(cls)
            inner.name = self.name
            setattr(outercls, self.name, inner)
        return cls


class inner(class_inner):
    """decorator for outer object attribute, inner derivation, carried inheritance
    and instance"""

    is_property = False
    is_cached = False

    @classmethod
    def property(cls, icls):
        """replicate standard @property decorator"""
        obj = cls(icls)
        obj.is_property = True
        return obj

    @classmethod
    def cached_property(cls, icls):
        """replicate sdtlib @cached_property decorator"""
        obj = cls(icls)
        obj.is_property = True
        obj.is_cached = True
        return obj

    def __get__(self, outerobj, outercls):
        icls = class_inner.__get__(self, outerobj, outercls)
        if outerobj is None:
            return icls
        # properties
        if self.is_property:
            innerobj = icls()
            innerobj.owner = outerobj
            if self.is_cached:
                setattr(outerobj, self.name, innerobj)
            return innerobj

        # constructor
        def ctor(*args, **kw):
            innerobj = icls.__new__(icls, *args, **kw)
            innerobj.owner = outerobj
            innerobj.__init__(*args, **kw)
            return innerobj

        update_wrapper(ctor, icls.__init__)
        return ctor
