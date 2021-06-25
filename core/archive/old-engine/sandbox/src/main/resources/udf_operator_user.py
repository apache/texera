

#This is a demo User define function, user need to implement it.
class UserTupleOperator(TupleOperator):
    def user_defined_function(self):
        global n2one
        global tag_output
        global my_length
        n2one = True
        if (n2one == True):
            #This demo will compute the total length of field content for all tuple 
            if (tag_input == b'0' and tag_output == 't'):
                #send end signal null
                tag_output = '0'
            if (tag_input == b'0' and tag_output == 'w'):
                #send signal to write tuple
                tag_output = 't'
                attrType =  "text"
                value = my_length
                new_attrName = "length"
                self.add_field(new_attrName, attrType, value)
            if (tag_input == 't' and tag_output == 'w'):
                #send signal to wait
                tag_output = 'w'
                #do caculation
                attrName = "content"
                attrType = "text"
                            
                value = len(self.get_valueByAttribute(attrName))
                self.output_tuple_dict = self.tuple_dict
                
                my_length = my_length + value
                    
        if(n2one == False):
            #we need to construct a output dict
            #this demo will compute the length of field "content"
            if (tag_output == 'w'):
                tag_output = 't'
            if (tag_input == 't'):
                tag_output = 't'
                #send signal to write tuple
                self.output_tuple_dict = self.tuple_dict
                attrName = "content"
                attrType = "text"
                value = len(self.get_valueByAttribute(attrName))
                new_attrName = "length"
                self.add_field(new_attrName, attrType, value)
                
            if (tag_input == b'0'):
                #send signal of null
                tag_output = '0'

def main():
    userTupleOperator = UserTupleOperator()
    signal.signal(signal.SIGUSR2, userTupleOperator.onsignal_usr2)

    while 1:
        sleep(1)
								
if __name__ == "__main__":
    # execute only if run as a script
    main()
