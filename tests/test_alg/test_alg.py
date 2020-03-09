def start(args, hkubeApi=None):
    print('start called')
    waiter1 = hkubeApi.start_algorithm('eval-alg', [5, 6], resultAsRaw=True)
    waiter2 = hkubeApi.start_algorithm(
        'green-alg', [6, 'stam'], resultAsRaw=True)
    res = [waiter1.get(), waiter2.get()]
    ret = list(map(lambda x: {'error': x.get('error')} if x.get(
        'error') != None else {'response': x.get('response')}, res))
    # ret='OK!!!'
    return ret