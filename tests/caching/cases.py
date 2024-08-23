from types import ModuleType

from hamilton.ad_hoc_utils import create_temporary_module


def module_1() -> ModuleType:
    """Base case: Node with no dependencies"""

    def A() -> int:
        return 1

    return create_temporary_module(A)


def module_2() -> ModuleType:
    """Base case: Node with single external dependency"""

    def A(external: int) -> int:
        return 1

    return create_temporary_module(A)


def module_3() -> ModuleType:
    """Node with more than one external dependency"""

    def A(external: int, external_2: int) -> int:
        return 1

    return create_temporary_module(A)


def module_4() -> ModuleType:
    """Node with single node dependency"""

    def A() -> int:
        return 1

    def B(A: int) -> int:
        return A + 3

    return create_temporary_module(A, B)


def module_5() -> ModuleType:
    """Node with node and external dependency"""

    def A() -> int:
        return 1

    def B(A: int, external: int) -> int:
        return A + external + 3

    return create_temporary_module(A, B)


def module_6() -> ModuleType:
    """Node with node and external dependency"""

    def A() -> int:
        return 1

    def B(A: int, external: int) -> int:
        return A + external + 3

    return create_temporary_module(A, B)


ALL_MODULES = [
    module_1(),
    module_2(),
    module_3(),
    module_4(),
    module_5(),
    module_6(),
]
