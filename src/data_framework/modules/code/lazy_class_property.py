class LazyClassProperty:

    def __init__(self, func):
        self.func = func
        self.attr_name = f"_{func.__name__}"

    def __get__(self, instance, owner):
        if not hasattr(owner, self.attr_name):
            print(f"Instancing '{self.func.__name__}'...")
            setattr(owner, self.attr_name, self.func(owner))
        return getattr(owner, self.attr_name)
