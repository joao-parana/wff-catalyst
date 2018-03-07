import multiprocessing as mp

def init_worker(mps, fps, cut):
    global memorizedPaths, filepaths, cutoff
    global DG
    print "process initializing", mp.current_process()
    memorizedPaths, filepaths, cutoff = mps, fps, cut
    DG = 1##nx.read_gml("KeggComplete.gml", relabel = True)

def work(item):
    _all_simple_paths_graph(DG, cutoff, item, memorizedPaths, filepaths)

def _all_simple_paths_graph(DG, cutoff, item, memorizedPaths, filepaths):
    memorizedPaths[item] = item
    # print "doing " + str(item) # pass

if __name__ == "__main__":
    m = mp.Manager()
    memorizedPaths = m.dict()
    filepaths = m.dict()
    cutoff = 1 ##
    # use all available CPUs
    p = mp.Pool(initializer=init_worker, initargs=(memorizedPaths,
                                                   filepaths,
                                                   cutoff))

    degreelist = range(200000000) ##
    for _ in p.imap_unordered(work, degreelist, chunksize=500):
        pass

    p.close()
    p.join()


