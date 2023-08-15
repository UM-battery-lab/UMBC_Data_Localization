class SingletonMeta(type):
    """
    A metaclass that ensures the singleton behavior for its instances.

    Singleton pattern ensures that a class has only one instance and provides
    a global point to access it. This metaclass enables such behavior for its 
    instances. Any class that uses this metaclass will adhere to the Singleton
    pattern.

    Attributes:
    -----------
    _instances : dict
        A dictionary to hold the single instance of each class that uses this 
        metaclass. The class itself is used as the key.

    Methods:
    --------
    __call__(*args, **kwargs) -> object:
        Overrides the default behavior of class instantiation. If a class instance 
        doesn't exist in _instances dict, it's created and stored. Otherwise, the 
        stored instance is returned.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Overrides the default behavior of class instantiation.

        If a class instance doesn't exist in _instances dict, it's created and 
        stored. Otherwise, the stored instance is returned.

        Parameters:
        -----------
        *args : tuple
            Variable length argument list.
        **kwargs : dict
            Arbitrary keyword arguments.

        Returns:
        --------
        object
            The single instance of the class.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]
