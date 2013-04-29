from OFS.Folder import Folder
from App.special_dtml import DTMLFile

class Member(Folder):
    member_id = ""
    name = ""
    address = ""
    phone = ''

    index_html = DTMLFile('employee_index_html', globals())