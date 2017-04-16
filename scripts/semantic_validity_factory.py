from __future__ import print_function

def _semantic_validity_factory(value, semantic_name, null_values=set(('Unspecified','N/A','',0,None)), **kwargs) :
    if value in null_values:
        return (None, None)
    elif all(f(value) for name, f in kwargs.items() if 'semantic' in name):
        if all(f(value) for name, f in kwargs.items() if 'valid' in name):
            return (semantic_name,'valid')
        else:
            return (semantic_name,'invalid')
    else:
        # only checking for semantic type!
        return (None, None)

def tester(check_list,test_type='all'):
    test_cases = {
        'strings':['10009','12345','Null','9171234567','2124445555','Foo'],
        'all':['10009',06,123456,'12345',12345,'(917)123-4567',
                'Null',None,'9171234567','2124445555','Foo',0,'Unspecified',
                'BROOKLYN','MANHATTAN','brooklyn',]
    }
    for i,check in enumerate(check_list):
        print('\n{}\n'.format(i))
        for test in test_cases[test_type]:
            try:
                print(test,check(test))
            except:
                print('*** -->TEST FAILED',test)