import sys

capacity_cache = 0
#id endpoint: object
endpoints = {}
#id cache : object
caches = {}
#id video: dimension
videos = {}

class Cache:
    def __init__(self, id):
        self.id = id
        self.endpoints_latency = {}
        self.endpoints = []
        self.rank_values = {}
        self.rank_list = []

    def register_endpoint(self, endpoint, latency):
        self.endpoints_latency[endpoint] = latency
        self.endpoints.append(endpoint)

    def add_rank_from_endpoint(self,video, points):
        self.rank_values[video] += points*len(self.rank_list)


class Endpoint:
    def __init__(self, id, datacenter_latency):
        self.id = id
        self.datacenter_latency = datacenter_latency
        #dict clusteid : latenza
        self.cache_latency = {}
        #id video: N* richeste
        self.requests = {}


def readFile(path):
    global capacity_cache
    global endpoints
    global caches
    global endpoints
    global videos
    f=open(path)
    # read first line
    first_line = f.readline()
    first_line_vec = first_line.split(' ')
    n_videos = int(first_line_vec[0])
    n_endpoints = int(first_line_vec[1])
    n_requests = int(first_line_vec[2])
    n_caches = int(first_line_vec[3])
    for i in range(n_caches):
        caches[str(i)] = Cache(str(i))
    capacity_cache = int(first_line_vec[4])

    # read second line: videos dimensions
    second_line = f.readline()
    second_line_vec = second_line.split(' ')
    second_line_int_vec = []
    for i in second_line_vec:
        second_line_int_vec.append(int(i))
    for vi in range(len(second_line_int_vec)):
        videos[str(vi)] = second_line_int_vec[vi]

    for i in range(0,n_endpoints):
        endid = str(i)
    # First line of an endpoint:
    # (latency vs datacenter) and (# of caches)
        first_end_line = f.readline().split()
        latency_to_datacenter = int(first_end_line[0])
        n_caches_connected = int(first_end_line[1])

        endpoints[endid]=Endpoint(endid,latency_to_datacenter)
        for j in range(0,n_caches_connected):
            a_line = f.readline().split()
            id_cache = a_line[0]
            latency_to_cache = latency_to_datacenter - int(a_line[1])
            endpoints[endid].cache_latency[id_cache] = latency_to_cache

    # Read number of request
    for i in range(0,n_requests):
        a_line = f.readline().split()
        id_video = a_line[0]
        endpoint_id_request = a_line[1]
        n_requests = int(a_line[2])
        endpoints[endpoint_id_request].requests[id_video] = n_requests


def populate_cache_endpoint_link():
    global capacity_cache
    global endpoints
    global caches
    global endpoints
    global videos
    for end in endpoints.values():
        for caches_id in end.cache_latency.keys():
            caches[caches_id].register_endpoint(end.id, end.cache_latency[caches_id])


def rank_video_cache(N, lat, mb):
    global capacity_cache
    global endpoints
    global caches
    global endpoints
    global videos
    return N / (lat * mb )


def calculate_cache_ranking():
    global capacity_cache
    global endpoints
    global caches
    global endpoints
    global videos
    for cache in caches.values():
        print("calculate_cache_ranking. cache {}".format(cache.id))
        for video in videos:
            rank_video= 0

            for endpoint_id in cache.endpoints:
                endpoint = endpoints[endpoint_id]
                if video in endpoint.requests:
                    n_of_request = endpoint.requests[video]
                    latency = cache.endpoints_latency[endpoint_id]
                    rank_video += rank_video_cache(n_of_request, latency, videos[video])

            cache.rank_values[video] = rank_video
        #ordering
        cache.rank_list = sorted(cache.rank_values,key=cache.rank_values.__getitem__, reverse=True )

def calculate_endpoint_ranking():
    global capacity_cache
    global endpoints
    global caches
    global endpoints
    global videos
    for endpoint in endpoints.values():
        print("calculate_endpoint_ranking. endpoint {}".format(endpoint.id))
        for video, Nrequests in endpoint.requests.items():
            cache_rank_values = {}

            for cache_id, cache_latency in endpoint.cache_latency.items():
                N_videos = len(caches[cache_id].rank_list)
                cache_rank_values[cache_id] = rank_video_cache(Nrequests, cache_latency,videos[video])/N_videos
            cache_rank_list = sorted(cache_rank_values, key=cache_rank_values.__getitem__, reverse=True )
            if len(cache_rank_list):
                best_cache_id = cache_rank_list[0]
                caches[best_cache_id].add_rank_from_endpoint(video, cache_rank_values[best_cache_id])

    #resort ranking in all caches
    for cache in caches.values():
        cache.rank_list = sorted(cache.rank_values,key=cache.rank_values.__getitem__, reverse=True  )

def get_final_result_per_cache():
    global capacity_cache
    global endpoints
    global caches
    global endpoints
    global videos
    result = {}
    for cache in caches.values():
        video_per_cache = []
        capacity = 0
        for v in cache.rank_list:
            if capacity < capacity_cache:
                if (videos[v]+capacity) < capacity_cache:
                    video_per_cache.append(v)
                    capacity += videos[v]
        result[cache.id] =  video_per_cache
    return result


if __name__ == "__main__":
    readFile(sys.argv[1])
    populate_cache_endpoint_link()
    calculate_cache_ranking()
    calculate_endpoint_ranking()
    result = get_final_result_per_cache()

    with open("results_{}.txt".format(sys.argv[1]),"w") as file:
        file.write(str(len(result))+ "\n")
        for c in result:
            file.write(c + " " + " ".join(result[c])+ "\n")
