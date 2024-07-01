'''
class DatabaseConnector

DatabaseConnector will be used to connect with and upload data to the database.
'''

class DatabaseConnector:
    '''
    Description

    
    Parameters:
    ----------
    parm: type
        description
    
    Attributes:
    ----------
    attribute: type
        description

    Methods:
    -------
    method()
        description
    '''
    def __init__(self, parm1, parm2):
        #pass

        self.parm1 = parm1
        self.parm2 = parm2
        self.attr1 = ''
        self.attr2 = ''

    def method1(self, parm1) -> None:
        '''
        Description

        Parameters:
        ----------
        parm: type
            description

        Returns:
        ----------
        parm: type
            description
        '''
        pass

        #return self.attr1, self.attr2

def read_db_creds() -> None:
    import yaml
    
    with open('db_creds.yaml') as f:
        # use safe_load instead load
        dataMap = yaml.safe_load(f)

    for key, value in dataMap.items():
        print(key, '\t', value)
    # print(dataMap.get('RDS_HOST'))
    # print(dataMap['RDS_HOST'])

if __name__ == '__main__':
    read_db_creds()