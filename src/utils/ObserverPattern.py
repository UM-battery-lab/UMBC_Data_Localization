# This file contains the implementation of the Observer Pattern

def Subject(cls):
    """
    The decorator to add the Subject functionality to a class
    """
    class Wrapped(cls):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._observers = []
        
        def attach(self, observer):
            self._observers.append(observer)
        
        def detach(self, observer):
            self._observers.remove(observer)
        
        def notify(self, *args, **kwargs):
            for observer in self._observers:
                observer.update(*args, **kwargs)
    return Wrapped

def Observer(cls):
    """
    The decorator to add the Observer functionality to a class
    """
    class Wrapped(cls):
        def update(self, *args, **kwargs):
            super().update(*args, **kwargs)
    return Wrapped