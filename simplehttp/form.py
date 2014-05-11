# coding: utf-8
import os, sys, string

class HTMLForm:
    def __init__(self):
        '''
        forms = {'form_name': [action, method, {'tag_name': [val, tag]}]}
        #tags = {'tag': [{'tag_name': [val, tag]}, ...]}
        '''
        self.forms = {} 
        
        self.current_form = None
    
    def add_form(self, name, action, method):
        if not name:
            name = 'form1'
        
        i = 0
        while self.forms.has_key(name):
            name += str(i)
            i += 1
        
        self.forms[name] = [action, method, {}]
        self.current_form = self.forms[name]
        return name

    def add_tag(self, form_name, name, val, tag):
        try:
            form = self.forms[form_name]
        except: 
            raise
        
        tags = form[2]
        tags[name] = [val, tag]

    def add_tag_cur(self, name, val, tag):
        tags = self.current_form[2]
        tags[name] = [val, tag]



    def remove_form(self, form_name):
        del self.forms[form_name]

    
    def remove_tag(self, form_name, tag_name):
        try:
            form = self.forms[form_name]
            del form[2][tag_name]
        except: 
            raise
         
    def clear(self):
        self.forms = {}

if __name__ == '__main__':
    form = HTMLForm()

    form.add_form("test1", "/login.php", 'POST')
    form.add_tag("test1", "user", "zhaowei", 'input')
    form.add_tag_cur("pass", "aaaaa", "input")

    print form.forms
