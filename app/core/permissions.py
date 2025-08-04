from typing import List

VIEW_ANALYTICS = "VIEW_ANALYTICS"
MANAGE_CAMPAIGNS = "MANAGE_CAMPAIGNS"
ADMIN = "ADMIN"


def has_permission(user_permissions: List[str], required_permission: str) -> bool:
    """
    Check if user has the required permission.
    ADMIN permission grants access to everything.
    """
    if ADMIN in user_permissions:
        return True
    return required_permission in user_permissions