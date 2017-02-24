import sys


videos = []
endpoints = []
requests = []
caches = []

CACHE_CAPACITY = None

class Cache(object):
    def __init__(self, _id):
        self._id = _id
        self.size = CACHE_CAPACITY
        self.requested_videos = {}  # contains elements of type { video_id: [RequestVideo]}
        self.cached_video_ids = set([])
        #self.queue.
        
    def add_request(self, video_request):
        if not video_request.video._id in self.requested_videos:
            self.requested_videos[video_request.video._id] = []
        
        self.requested_videos[video_request.video._id].append(video_request)
        
    def get_video_speedup(self, video_id):
        if videos[video_id].size > self.size:
            return -1

        total = 0
        for video_req in self.requested_videos[video_id]:
            total += video_req.get_score_for_cache_id(self._id)
            
        return total
        
    def add_video_to_cache(self, video_id):
        if video_id in self.cached_video_ids:
            return
        
        self.cached_video_ids.add(video_id)
        self.size -= videos[video_id].size
        
        for req in self.requested_videos[video_id]:
            req.update_minimal_latency(self._id)
            
        del self.requested_videos[video_id]
    
    def get_best_video_by_speedup(self):
        if not self.requested_videos:
            return None
        best_video_id = max(self.requested_videos.keys(), key=self.get_video_speedup)
        
        if self.get_video_speedup(best_video_id) == -1:
            return None
        #print "best video id %s in cache %s" % (best_video_id, self._id)
        
        return best_video_id
        
    def add_best_videos(self):
        video_ids = self.requested_videos.keys()
        sorted_video_ids = sorted(video_ids, key=self.get_video_speedup)
        
        for video_id in sorted_video_ids:
            if videos[video_id] > self.size:
                return
            self.add_video_to_cache(video_id)
    
    def __str__(self):
        return "<Cache(%s) (free %s/%s MB)>" % (self._id, self.size, CACHE_CAPACITY)
        

class Video(object):
    def __init__(self, _id, size):
        self._id = _id
        self.size = size
    
    def __str__(self):
        return "<Video(%s) %s MB>" % (self._id, self.size)


class Endpoint(object):
    def __init__(self, _id, datacenter_time):
        self._id = _id
        self.datacenter_time = datacenter_time
        self.caches = {}
        
    def add_cache(self, cache_id, latency):
        self.cache_id = latency
        
    def get_cache_latency(self, cache_id):
        return self.caches[cache_id]

    def __str__(self):
        return "<Endpoint(%s) TimeToDataCenter(%s ms)>" % (self._id, self.datacenter_time)
        

class RequestVideo(object):
    def __init__(self, size, video, endpoint):
        self.size = size
        self.video = video
        self.endpoint = endpoint
        self.minimal_latency = endpoint.datacenter_time

    def get_score_for_cache_id(self, cache_id):
        normal_latency = self.endpoint.get_cache_latency(cache_id)
        speedup = self.size * (self.minimal_latency - normal_latency)
        return speedup
        
    def update_minimal_latency(self, cache_id):
        new_latency = self.endpoint.get_cache_latency(cache_id)
        if self.minimal_latency > new_latency:
            self.minimal_latency = new_latency
    
    def __str__(self):
        return "<%s RequestVideo of %s from %s %s>" % (self.size, self.video, self.endpoint, self.minimal_latency)


def read_input(filename_in):
    def get_numbers_from_line(line):
        return map(int, line.split(' '))

    global CACHE_CAPACITY
    with open(filename_in) as f:
        V, E, R, C, CACHE_CAPACITY = get_numbers_from_line(f.readline())
        #print V, E, R, C, CACHE_CAPACITY
        
        global caches
        caches = [Cache(_id) for _id in range(C)]
        
        # Read videos and their sizes
        
        video_sizes = get_numbers_from_line(f.readline())
        global videos
        videos = [Video(video_id, video_size)
            for video_id, video_size in enumerate(video_sizes)
        ]
        
        # Read endpoints and their latencies to connected caches

        for endpoint_id in range(E):
            datacenter_time, num_caches_connected_to = get_numbers_from_line(f.readline())
            endpoint = Endpoint(endpoint_id, datacenter_time)
            
            for i in range(num_caches_connected_to):
                cache_id, latency = get_numbers_from_line(f.readline())
                endpoint.caches[cache_id] = latency
            
            endpoints.append(endpoint)

        for i in range(R):
            video_id, endpoint_id, num_requests = get_numbers_from_line(f.readline())
            rv = RequestVideo(num_requests, videos[video_id], endpoints[endpoint_id])
            
            for cache_id in rv.endpoint.caches.keys():
                cache = caches[cache_id]
                cache.add_request(rv) 


def print_input():
    for e in endpoints:
        print e
        print "caches of endpoint", e.caches
        print ""
    for v in videos:
        print v

    for r in requests:
        print r
    
    for c in caches:
        print c
        print "requested",
        for r in c.requested_videos:
            print r,
        print ""


def main(filename_in, filename_out):
    read_input(filename_in)
    #print_input()
    
    for cache in caches:
        cache.add_best_videos()
    
    # output
    used_caches = filter(lambda cache: len(cache.cached_video_ids), caches)
    with open(filename_out, "w") as f:
        f.write("%s\n" % str(len(used_caches)))
        for cache in used_caches:
            videos_contained = " ".join(map(str, cache.cached_video_ids))
            f.write("%s %s\n" % (cache._id, videos_contained))

if __name__ == '__main__':
    if len(sys.argv) < 3:
        raise Exception("Please provide an input and output file name")
    main(sys.argv[1], sys.argv[2])

