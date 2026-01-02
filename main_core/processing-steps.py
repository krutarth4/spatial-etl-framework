import dataclasses
from typing import Callable, List


@dataclasses.dataclass(order=True)
class StepDTO:
    priority: float
    description: str
    callable: Callable | None




class ProcessingSteps:
    _steps: List[StepDTO] = []


    def __init__(self):
        pass

    def _add_step(self, step: StepDTO) -> None:
        if not isinstance(step, StepDTO):
            raise TypeError("Step must be of type StepDTO")
        self._steps.append(step)
        self._steps.sort(key=lambda step: step.priority)

    def _get_steps(self):
        return list(self._steps)


