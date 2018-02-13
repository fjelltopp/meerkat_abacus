import datetime
from meerkat_abacus import model

class ProcessingStep:
    """
    Base class for all ProcessingSteps

    """

    def __init__(self):
        pass

    def run(self):
        raise NotImplementedError

    def start_step(self):
        self.start = datetime.datetime.now()

    def end_step(self, n):
        end = datetime.datetime.now()
        duration = (end - self.start).total_seconds()
        write_monitoring_data(self.session,
                              start=self.start,
                              end=end,
                              duration=duration,
                              step=self.step_name,
                              n=n)


def write_monitoring_data(session,
                          start=None,
                          end=None,
                          duration=None,
                          step=None,
                          n=None):
    monitoring = model.StepMonitoring(
        step=step,
        end=end,
        start=start,
        duration=duration,
        n=n)
    session.add(monitoring)
    session.commit()
        
        

class DoNothing:
    def __init__(self):
        pass
    
    def run(self, form, data):
        return [{"form": form,
                 "data": data}]

        
