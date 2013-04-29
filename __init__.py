from container import MemberContainer
from OFS.Folder import Folder
from App.special_dtml import DTMLFile

manage_ZeSearch_form = DTMLFile("views/main_page", globals())

def manage_setUpZeSearch(context):
    """setup zesearch"""
    ze_search = MemberContainer()
    ze_id = context.REQUEST['obj_id']
    ze_search.add_member_container()
    context._setObject(ze_id, ze_search)
    context.REQUEST.RESPONSE.redirect("./manage_main")

def initialize(context):
    context.registerClass(MemberContainer,
            constructors=(manage_ZeSearch_form, manage_setUpZeSearch,)
            )
