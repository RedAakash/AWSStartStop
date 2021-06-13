import boto3
import datetime as dt
from time import ctime

default_profile = 'ProfileName'
default_region = 'ap-south-1'   # Example AWS Code Region ap-south-1

logfile_location = '/tmp/logfile.log'

default_cluster_names_list = [
    # 'cluster_names'
] ### If you want to start & stop some sort of services mention in the same list | Otherwise run for All Clusters and ECS Services

start_time = dt.time(22,00)
end_time = dt.time(10,00)

class BotoConnetion:
    def __init__(self, profile_name : str = default_profile, region_name : str = default_region, *args, **kwargs):
        self.profile_name = profile_name
        self.region_name = default_region
        
        self.connection = boto3.Session(
            profile_name = self.profile_name
        ).client(
            'ecs',
            region_name = self.region_name
        )
        
    def time_calculate(self, startTime=start_time, endTime=end_time, nowTime=dt.datetime.now().time()):
        if nowTime >= startTime or nowTime <= endTime or dt.datetime.today().weekday() > 4:
            return True
        return False
        
    def get_list_of_clusters(self, *args, **kwargs):
        clusters = self.connection.list_clusters(
            maxResults=50
        )
        clustersArns = clusters['clusterArns']
        clustersDescriptions = self.connection.describe_clusters(
            clusters=clustersArns
        )
        
        return [
            cluster['clusterName'] for cluster in clustersDescriptions['clusters']
        ]
        
    def get_clusters(self, cluster_names_list : list = [], *args, **kwargs):
        return self.get_list_of_clusters() if not cluster_names_list else cluster_names_list
    
    def get_dict_of_services(self, cluster_list : list = [], *args, **kwargs):
        cluster_services_dict = dict()
        
        clusters = self.get_clusters(
            cluster_names_list = cluster_list
        )
        
        for cluster_name in clusters:
            try:
                servicesArns = self.connection.list_services(
                    cluster = cluster_name
                )['serviceArns']
                
                servicesDescriptions = self.connection.describe_services(
                    cluster = cluster_name,
                    services = servicesArns
                )
                
                cluster_services_dict[cluster_name] = [ service['serviceName'] for service in servicesDescriptions['services'] ]
            except Exception as err:
                print('Not Found ECS Cluster => {cluster}'.format(cluster=cluster_name))

        return cluster_services_dict
        
    def get_dict_of_tasks(self, cluster_list : list = [], *args, **kwargs):
        cluster_running_tasks = dict()
        
        clusters = self.get_clusters(
            cluster_names_list = cluster_list
        )
        
        for cluster_name in clusters:
            try:
                cluster_running_tasks[cluster_name] = self.connection.list_tasks(
                    cluster = cluster_name,
                    desiredStatus = 'RUNNING'
                )
            except Exception as err:
                print('Not Found ECS Cluster => {cluster}'.format(cluster=cluster_name))

        return cluster_running_tasks
        
    def update_services_stop(self, cluster_list : list = [], *args, **kwargs):
        services_dict = self.get_dict_of_services(cluster_list)
        
        for cluster_name, services_obj in services_dict.items():
            for service_name in services_obj:
                try:
                    update_service = self.connection.update_service(
                        cluster = cluster_name,
                        service = service_name,
                        desiredCount = 0
                    )
                
                except Exception as err:
                    msg = f'Error In this Service => Cluster: {cluster_name}, Service: {service_name}\n'
                
                else:
                    msg = f'this Service => Cluster: {cluster_name}, Service: {service_name} stopped time: {ctime()}\n'
                
                finally:
                    with open(logfile_location, 'a') as fileLog:
                        fileLog.write(msg)
    
    def update_services_start(self, cluster_list : list = [], *args, **kwargs):
        services_dict = self.get_dict_of_services(cluster_list)
        
        for cluster_name, services_obj in services_dict.items():
            for service_name in services_obj:
                try:
                    update_service = self.connection.update_service(
                        cluster = cluster_name,
                        service = service_name,
                        desiredCount = 1
                    )
            
                except Exception as err:
                    msg = f'Error In this Service => Cluster: {cluster_name}, Service: {service_name}\n'
                
                else:
                    msg = f'this Service => Cluster: {cluster_name}, Service: {service_name} starting time: {ctime()}\n'
                
                finally:
                    with open(logfile_location, 'a') as fileLog:
                        fileLog.write(msg)
        
    def __call__(self, date_time_bool : bool, cluster_list : list = [], *args, **kwargs):
        services = self.update_services_stop(cluster_list)
        if date_time_bool:
            services = self.update_services_stop(cluster_list)
        else:
            services = self.update_services_start(cluster_list)

############ Don't Change Here
def main():
    BotoInstance = BotoConnetion()
    BotoInstance(
        date_time_bool = BotoInstance.time_calculate(),
        cluster_list = default_cluster_names_list
    ) ### execute class function __call__()

if __name__ == '__main__':
    main()
