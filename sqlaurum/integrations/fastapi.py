import inspect

from fastapi import Depends

from ..repository import SQLAlchemyModelRepository


def depends_on(get_db):
    def wrapper(cls: SQLAlchemyModelRepository):
        old_signature = inspect.signature(cls.__init__)  # type: ignore
        old_parameters: list[inspect.Parameter] = list(
            old_signature.parameters.values()
        )
        old_first_parameter = old_parameters[0]
        old_second_parameter = old_parameters[1]
        new_first_parameter = old_second_parameter.replace(default=Depends(get_db))
        new_parameters = [old_first_parameter, new_first_parameter] + [
            parameter.replace(kind=inspect.Parameter.KEYWORD_ONLY)
            for parameter in old_parameters[2:]
        ]
        new_signature = old_signature.replace(parameters=new_parameters)
        setattr(cls.__init__, "__signature__", new_signature)  # type: ignore
        return cls

    return wrapper
