from abc import ABC, abstractmethod

class Observer(ABC):
    """
    The Observer interface declares the update method, used by subjects.

    Methods
    -------
    update(*args, **kwargs)
        Receive update from subject.
    """
    @abstractmethod
    def update(self, *args, **kwargs):
        pass

class Subject(ABC):
    """
    The Subject interface declares a set of methods for managing subscribers.

    Methods
    -------
    attach(observer: Observer)
        Attach an observer to the subject.
    detach(observer: Observer)
        Detach an observer from the subject.
    notify(*args, **kwargs)
        Notify all observers about an event.
    """
    @abstractmethod
    def attach(self, observer: Observer):
        pass
    
    @abstractmethod
    def detach(self, observer: Observer):
        pass
    
    @abstractmethod
    def notify(self, *args, **kwargs):
        pass
