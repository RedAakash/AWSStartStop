import boto3
import datetime as dt
from time import ctime, sleep
import threading

default_profile = 'AwsProfile'
default_region = 'ap-south-1'

default_cluster_names_list = [
    # 'default-cluster'
]

logfile_location = '/tmp/LoggingFile.log'

default_instances_ids_list = [
    #'i-03984cc6'
]

default_rds_db_instance_identifier = [
    #'default-rds'
]

start_time = dt.time(22,00)
end_time = dt.time(9,00)

class BotoConnetion:
    def __init__(self, profile_name : str = default_profile, region_name : str = default_region, *args, **kwargs):
        self.profile_name = profile_name
        self.region_name = default_region
        
        self.ecs_connection = boto3.Session(
            profile_name = self.profile_name
        ).client(
            'ecs',
            region_name = self.region_name
        )
        
        self.ec2_connection = boto3.Session(
            profile_name = self.profile_name
        ).client(
            'ec2',
            region_name = self.region_name
        )
        
        self.rds_connection = boto3.Session(
            profile_name = self.profile_name
        ).client(
            'rds',
            region_name = self.region_name
        )
        
        self.rds_instances_status = dict()
        self.rds_thread_list = []
        
    def time_calculate(self, startTime=start_time, endTime=end_time, nowTime=dt.datetime.now().time()):
        if nowTime >= startTime or nowTime <= endTime or dt.datetime.today().weekday() > 4:
            return True
        return False
        
    def log_write(self, msg : str, *args, **kwargs):
        with open(logfile_location, 'a') as fileLog:
            fileLog.write(msg)
    
    def get_rds_instances_list(self, rds_db_instance_identifier : list = [], *args, **kwargs):
        if not default_rds_db_instance_identifier:
            return [ rds_instances_ids['DBInstanceIdentifier'] for rds_instances_ids in self.rds_connection.describe_db_instances()['DBInstances']]
        return rds_db_instance_identifier
    
    def get_rds_instances_status(self, rds_db_instance_identifier : str, *args, **kwargs):
        instance_status = "available"
        self.log_write(msg = f'Starting Thread {threading.currentThread().name} , Time {ctime()}\n')
        while True:
            sleep(10)
            instanceStatus = self.rds_connection.describe_db_instances(
                DBInstanceIdentifier = rds_db_instance_identifier
            )['DBInstances'][0]['DBInstanceStatus']
            self.log_write(msg = f'In Progress Thread {threading.currentThread().name} , RDS Status => {instanceStatus}, Time {ctime()}\n')
            if instanceStatus == instance_status:
                self.rds_instances_status[rds_db_instance_identifier] = instanceStatus
                break
        self.log_write(msg = f'Finished Thread {threading.currentThread().name} , Time {ctime()}\n')
        
    def stop_rds_instances(self, rds_db_instance_identifier : list = [], *args, **kwargs):
        rds_instance_identifier_list = self.get_rds_instances_list(rds_db_instance_identifier)
        
        for db_identifier in rds_instance_identifier_list:
            try:
                self.rds_connection.stop_db_instance(
                    DBInstanceIdentifier = db_identifier
                )
            except Exception as err:
                msg = f'Error RDS on stopping => {db_identifier} | time {ctime()}'
            else:
                msg = f'RDS stop => {db_identifier} | time {ctime()}'
            finally:
                self.log_write(msg)
    
    def start_rds_instances(self, rds_db_instance_identifier : list = [], *args, **kwargs):
        rds_instance_identifier_list = self.get_rds_instances_list(rds_db_instance_identifier)
        
        for db_identifier in rds_instance_identifier_list:
            try:
                self.rds_connection.start_db_instance(
                    DBInstanceIdentifier = db_identifier
                )
            except Exception as err:
                msg = f'Error RDS on START => {db_identifier} | time {ctime()}'
            else:
                msg = f'RDS START => {db_identifier} | time {ctime()}'
            finally:
                self.log_write(msg)
    
    def get_instance_ids(self, *args, **kwargs):
        all_instances = self.ec2_connection.describe_instances()
        instance_ids = []
        for reservation in all_instances['Reservations']:
            for instance in reservation['Instances']:
                if 'Tags' in instance:
                    for tag in instance['Tags']:
                        if tag['Key'] == 'Name':
                            instance_ids.append(instance['InstanceId'])
        return instance_ids
    
    def get_instances(self, instances_ids : list = [], *args, **kwargs):
        return self.get_instance_ids() if not instances_ids else instances_ids
    
    def stop_instances(self, instances_ids : list = [], *args, **kwargs):
        instances_ids = self.get_instances(instances_ids)
        try:
            stop_instances = self.ec2_connection.stop_instances(
                InstanceIds = instances_ids
            )
        except Exception as err:
            msg = f'Error EC2 instances stop {ctime()}\n'
        else:
            msg = f'EC2 instances stop {ctime()}\n'
        finally:
            self.log_write(msg)
        
    def start_instances(self, instances_ids : list = [], *args, **kwargs):
        instances_ids = self.get_instances(instances_ids)
        try:
            stop_instances = self.ec2_connection.start_instances(
                InstanceIds = instances_ids
            )
        except Exception as err:
            msg = f'Error EC2 instances stop {ctime()}\n'
        else:
            msg = f'EC2 instances stop {ctime()}\n'
        finally:
            self.log_write(msg)
        
    def get_list_of_clusters(self, *args, **kwargs):
        clusters = self.ecs_connection.list_clusters(
            maxResults=50
        )
        clustersArns = clusters['clusterArns']
        clustersDescriptions = self.ecs_connection.describe_clusters(
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
                servicesArns = self.ecs_connection.list_services(
                    cluster = cluster_name
                )['serviceArns']
                
                servicesDescriptions = self.ecs_connection.describe_services(
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
                cluster_running_tasks[cluster_name] = self.ecs_connection.list_tasks(
                    cluster = cluster_name,
                    desiredStatus = 'RUNNING'
                )
            except Exception as err:
                print('Not Found ECS Cluster => {cluster}'.format(cluster=cluster_name))

        return cluster_running_tasks
        
    def update_ecs_services_stop(self, cluster_list : list = [], *args, **kwargs):
        services_dict = self.get_dict_of_services(cluster_list)
        
        for cluster_name, services_obj in services_dict.items():
            for service_name in services_obj:
                try:
                    update_service = self.ecs_connection.update_service(
                        cluster = cluster_name,
                        service = service_name,
                        desiredCount = 0
                    )
                
                except Exception as err:
                    msg = f'Error In this Service => Cluster: {cluster_name}, Service: {service_name}\n'
                
                else:
                    msg = f'this Service => Cluster: {cluster_name}, Service: {service_name} stopped time: {ctime()}\n'
                
                finally:
                    self.log_write(msg)
    
    def update_ecs_services_start(self, cluster_list : list = [], *args, **kwargs):
        services_dict = self.get_dict_of_services(cluster_list)
        
        for cluster_name, services_obj in services_dict.items():
            for service_name in services_obj:
                try:
                    update_service = self.ecs_connection.update_service(
                        cluster = cluster_name,
                        service = service_name,
                        desiredCount = 1
                    )
            
                except Exception as err:
                    msg = f'Error In this Service => Cluster: {cluster_name}, Service: {service_name}\n'
                
                else:
                    msg = f'this Service => Cluster: {cluster_name}, Service: {service_name} starting time: {ctime()}\n'
                
                finally:
                    self.log_write(msg)
    
    def rds_thread_function(self, rds_db_instance_identifier : list = [], *args, **kwargs):
        rds_instances_names_obj = self.get_rds_instances_list(rds_db_instance_identifier)
        
        for rds_instance_name in rds_instances_names_obj:
            threads = threading.Thread(target=self.get_rds_instances_status, args=(rds_instance_name,), name=f'thread_{rds_instance_name}')
            threads.start()
            self.rds_thread_list.append(threads)
        
    def __call__(self, date_time_bool : bool, cluster_list : list = [], instances_ids_list : list = [], rds_db_instance_identifier : list = [],  *args, **kwargs):
        if date_time_bool:
            rds_instance = self.stop_rds_instances(rds_db_instance_identifier, *args, **kwargs)
            services = self.update_ecs_services_stop(cluster_list, *args, **kwargs)
            instances = self.stop_instances(instances_ids_list, *args, **kwargs)
        else:
            instances = self.start_instances(instances_ids_list, *args, **kwargs)
            rds_instance = self.start_rds_instances(rds_db_instance_identifier, *args, **kwargs)
            self.rds_thread_function(rds_db_instance_identifier)
            for i in self.rds_thread_list:
                i.join()
            services = self.update_ecs_services_start(cluster_list, *args, **kwargs)

############ Don't Change Here
def main():
    BotoInstance = BotoConnetion()
    BotoInstance(
        date_time_bool = BotoInstance.time_calculate(),
        cluster_list = default_cluster_names_list,
        instances_ids_list = default_instances_ids_list,
        rds_db_instance_identifier = default_rds_db_instance_identifier
    ) ### execute class function __call__()

if __name__ == '__main__':
    main()
