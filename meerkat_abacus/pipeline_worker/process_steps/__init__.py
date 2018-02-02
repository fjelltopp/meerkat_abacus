class ProcessingStep:
    """
    Base class for all ProcessingSteps

    """

    def __init__(self):
        pass

    def run(self):
        raise NotImplementedError
    

class DoNothing:
    def __init__(self):
        pass
    
    def run(self, form, data):
        return [{"form": form,
                 "data": data}]

        
