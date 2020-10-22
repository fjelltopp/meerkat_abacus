from abc import abstractmethod
import cProfile, pstats, io

import datetime
from meerkat_abacus import model
from meerkat_abacus import logger


class ProcessingStep(object):
    """
    Base class for all ProcessingSteps

    """

    def __init__(self):
        self.step_name = "processing_step"
        self.session = None
        self.start = None
        self.end = None
        
    def __repr__(self):
        return f'<{self.__class__}, step_name="{self.step_name}">'

    @property
    def duration(self):
        if not self.start or not self.end:
            return None
        return self.end - self.start

    @abstractmethod
    def run(self, form, data):
        pass

    def start_step(self):
        self.start = datetime.datetime.now()
        # self.profiler = cProfile.Profile()
        # self.profiler.enable()

    def end_step(self, n):
        self.end = datetime.datetime.now()
        self._write_monitoring_data(n)
        # self.profiler.disable()
        s = io.StringIO()
        sortby = 'cumulative'
        # ps = pstats.Stats(self.profiler, stream=s).sort_stats(sortby)
        # ps.print_stats(30)
        # logger.info(s.getvalue())
        
    def _write_monitoring_data(self, n=None):
        monitoring = model.StepMonitoring(
            step=self.step_name,
            end=self.end,
            start=self.start,
            duration=self.duration.total_seconds(),
            n=n)
        self.session.add(monitoring)
        self.session.commit()


class DoNothing(ProcessingStep):
    def __init__(self, session):
        super().__init__()
        self.step_name = "do_nothing"
        self.session = session

    def run(self, form, data):
        return [{"form": form,
                 "data": data}]
