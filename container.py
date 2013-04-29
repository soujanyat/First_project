from OFS.Folder import Folder
from App.special_dtml import DTMLFile
from member import Member
from edit_member import EditMember
from sqlalchemy import create_engine
from sqlalchemy import MetaData,Table,Column,Sequence,Integer,String,ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
import sqlite3
import sqlalchemy
import time
import datetime

engine = create_engine('sqlite:////tsoujanya/zeserch.db', echo=True)

metadata = MetaData()
Base = declarative_base()


class Members(Base):
    __tablename__ = 'mbr'
    id = Column(Integer, Sequence('mbr_id_seq'), primary_key=True)
    name = Column(String(20))
    address = Column(String(30))
    phone = Column(Integer(10))


Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
Session.configure(bind=engine)
session=Session() 


class MemberContainer(Folder):
    meta_type = "ZeSearch"
    index_html = DTMLFile('views/index_html', globals())
    add_member = DTMLFile('views/add_member', globals())
    edit_member = DTMLFile('views/edit_member', globals())
    search_contact = DTMLFile('views/search_page', globals())
    search_view = DTMLFile('views/search_view', globals())


    def get_all_members(self):
        """ """
        member_list = session.query(Members).all()
        return member_list

    def get_member(self):
        """ """
        id = self.REQUEST['member_id']
        contacts = session.query(Members).filter(Member.id==id).one()
        return [contacts]

    def add_member_container(self):
        """Add Member """
        self._setObject('member', MemberContainer())

    def insert_member(self):
        """ """
        request = self.REQUEST
        mbr = Members()
        mbr.name = request['name']
        mbr.address = request['address']
        mbr.phone = request['phone']
        session.add(mbr)
        session.commit()
        request.RESPONSE.redirect("index_html")

    def edit_member_details(self):
        """
        """
        request = self.REQUEST
        id = request['member_id']
        mbr = session.query(Members).filter(Members.id==id).one()
        mbr.name = request['member_name']
        mbr.address = request['member_address']
        mbr.phone = request['phone']
        session.add(mbr)
        session.commit()
        request.RESPONSE.redirect('index_html')

    def delete_contact(self):
        """ """
        request = self.REQUEST
        id= request['member_id']
        mbr = session.query(Members).filter(Members.id==id).one()
        session.delete(mbr)
        session.commit()
        request.RESPONSE.redirect(" ")

    def search_contacts(self):
        """
        """
        request = self.REQUEST
        id = request['member_id']
        search_list=[]
        if id:
            mbr=session.query(Members).filter(Members.id==id).one()
            search_list.append(mbr)
            search_list = search_list + mbr
        search_list=set(search_list)
        return self.search_view(self,request,search_list=list(search_list))
    
    
    #def search_contacts(self):
        #"""
#"""
        #request = self.REQUEST
        #member_id = request['member_id']
        #search_list=[]
        #if member_id:
            #search_list.append(member_id)
        #search_list=set(search_list)
        #return self.search_view(self,request,search_list=list(search_list))

    #def edit_member_details(self):
        #"""
        #"""
        #request = self.REQUEST
        #member_id = request['member_id']
        #member_name = request['member_name']
        #member_address = request['member_address']
        #member = self.member[member_id]
        #member.id = member
        #member.name = member_name
        #member.address = member_address
        #request.RESPONSE.redirect('index_html')

    #def delete_contact(self):
        #""" """
        #request = self.REQUEST
        #member_id= request['member_id']
        #self.member._delObject(member_id)
        #request.RESPONSE.redirect("index_html")

    #def search_contacts(self):
        #"""
#"""
        #request = self.REQUEST
        #member_id = request['member_id']
        #search_list=[]
        #if member_id:
            #search_list.append(member_id)
        #search_list=set(search_list)
        #return self.search_view(self,request,search_list=list(search_list))


    #if member_id:
            #mbr=session.query(Mbr).filter(Mbr.id==mbr_id).one()
            #search_list.append(mbr)
        #if member_name:
            #mbr_list=session.query(Mbr).filter(Mbr.name==name).all()
            #search_list = search_list + mbr_list
        #search_list=set(search_list)
        #return self.search_view_contact(self,request,search_list=list(search_list))
