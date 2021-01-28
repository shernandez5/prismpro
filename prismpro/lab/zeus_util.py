#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
# Author: frank_yang@nutanix.com (Frank Yang)
#
# Zeus util used to set up ui.
#
import gflags
import google.protobuf.text_format as text_format

from prism_client.proto.cluster_config_pb2 import *
from util.base import log
from util.net.ssh_client import SSHClient
from zeus.zookeeper_session import ZookeeperSession
from zeus.configuration_pb2 import ConfigurationProto

FLAGS = gflags.FLAGS

SECS_TO_USECS = 10 ** 6

EXTERNAL_CLUSTER_PATH = "/appliance/physical/clusterexternalstate"
ZEUSCONFIG_CLUSTER_PATH = "/appliance/physical/zeusconfig"
STATE_CLUSTER_PATH = "/appliance/physical/clusterdatastate"

CONFIG = """
logical_timestamp: 0
rackable_unit_list {
  rackable_unit_id: 3
  rackable_unit_model: 100
  rackable_unit_model_name: "NX-3060-G5"
  rackable_unit_uuid: "0cfd21c9-c941-4772-84dd-2caec6cb5a53"
}
node_list {
  service_vm_id: 5
  node_status: kNormal
  service_vm_external_ip: "10.1.1.2"
  cassandra_token_id: "FV000000oxDzDNIwiRf60E7wT0NJJxWgpdDH7DFjxZ9q2Lqx5sdXYyAnhTaq"
  zookeeper_myid: 2
  uuid: "825a0ffc-0dfa-499c-b49e-143102a4bc31"
  rackable_unit_id: 3
  node_position: 1
  cassandra_status: kNormalMode
  software_version: "el7.3-release-euphrates-5.15.3-stable-90bf256ce6fc4ad9cfd12b2ef046074490904e83"
  node_serial: "ZM144S037403"
  management_server_id: 64
  acropolis_status {
    conn_state: kConnected
    locality_restored: true
  }
  last_known_metadata_disk_id: 69
  cassandra_compaction_pending: false
  cluster_uuid: "825a0ffc-0dfa-499c-b49e-143102a4bc3c"
  rackable_unit_uuid: "0cfd21c9-c941-4772-84dd-2caec6cb5a53"
  management_server_uuid: "f829fb6f-256c-4e4c-926b-d5e469c62139"
}
node_list {
  service_vm_id: 6
  node_status: kNormal
  service_vm_external_ip: "10.1.1.3"
  cassandra_token_id: "FV000000oxDzDNIwiRf60E7wT0NJJxWgpdDH7DFjxZ9q2Lqx5sdXYyAnhTaq"
  zookeeper_myid: 2
  uuid: "825a0ffc-0dfa-499c-b49e-143102a4bc32"
  rackable_unit_id: 3
  node_position: 1
  cassandra_status: kNormalMode
  software_version: "el7.3-release-euphrates-5.15.3-stable-90bf256ce6fc4ad9cfd12b2ef046074490904e83"
  node_serial: "ZM144S037403"
  management_server_id: 64
  acropolis_status {
    conn_state: kConnected
    locality_restored: true
  }
  last_known_metadata_disk_id: 69
  cassandra_compaction_pending: false
  cluster_uuid: "825a0ffc-0dfa-499c-b49e-143102a4bc3c"
  rackable_unit_uuid: "0cfd21c9-c941-4772-84dd-2caec6cb5a53"
  management_server_uuid: "f829fb6f-256c-4e4c-926b-d5e469c62139"
}
node_list {
  service_vm_id: 7
  node_status: kNormal
  service_vm_external_ip: "10.1.1.4"
  cassandra_token_id: "FV000000oxDzDNIwiRf60E7wT0NJJxWgpdDH7DFjxZ9q2Lqx5sdXYyAnhTaq"
  zookeeper_myid: 2
  uuid: "825a0ffc-0dfa-499c-b49e-143102a4bc33"
  rackable_unit_id: 3
  node_position: 2
  cassandra_status: kNormalMode
  software_version: "el7.3-release-euphrates-5.15.3-stable-90bf256ce6fc4ad9cfd12b2ef046074490904e83"
  node_serial: "ZM144S037403"
  management_server_id: 64
  acropolis_status {
    conn_state: kConnected
    locality_restored: true
  }
  last_known_metadata_disk_id: 69
  cassandra_compaction_pending: false
  cluster_uuid: "825a0ffc-0dfa-499c-b49e-143102a4bc3c"
  rackable_unit_uuid: "0cfd21c9-c941-4772-84dd-2caec6cb5a53"
  management_server_uuid: "f829fb6f-256c-4e4c-926b-d5e469c62139"
}
node_list {
  service_vm_id: 8
  node_status: kNormal
  service_vm_external_ip: "10.1.1.5"
  cassandra_token_id: "FV000000oxDzDNIwiRf60E7wT0NJJxWgpdDH7DFjxZ9q2Lqx5sdXYyAnhTaq"
  zookeeper_myid: 2
  uuid: "825a0ffc-0dfa-499c-b49e-143102a4bc34"
  rackable_unit_id: 3
  node_position: 1
  cassandra_status: kNormalMode
  software_version: "el7.3-release-euphrates-5.15.3-stable-90bf256ce6fc4ad9cfd12b2ef046074490904e83"
  node_serial: "ZM144S037403"
  management_server_id: 64
  acropolis_status {
    conn_state: kConnected
    locality_restored: true
  }
  last_known_metadata_disk_id: 69
  cassandra_compaction_pending: false
  cluster_uuid: "825a0ffc-0dfa-499c-b49e-143102a4bc3c"
  rackable_unit_uuid: "0cfd21c9-c941-4772-84dd-2caec6cb5a53"
  management_server_uuid: "f829fb6f-256c-4e4c-926b-d5e469c62139"
}
release_version: "el7.3-release-euphrates-5.15.3-stable-90bf256ce6fc4ad9cfd12b2ef046074490904e83"
cluster_fault_tolerance_state {
  current_max_fault_tolerance: 1
  desired_max_fault_tolerance: 1
}
acropolis_ha_config {
  failover_enabled: false
  num_host_failures_to_tolerate: 0
}
cluster_functions: 1
cluster_uuid: "00057d50-00df-b390-0000-00000000eafd"
cluster_name: "Prism-Pro-Cluster"
extended_nfs_fhandle_enabled: true
ncc_version: "ncc-999.0.0"
"""

class ZeusUtil:

  def __init__(self, ip):
    self.ip = ip
    self.zks = ZookeeperSession(ip + ":9876")
    if not self.zks.wait_for_connection(20):
      log.FATAL("Failed to connect to zookeeper - Check zookeeper config.")
      return False
    if self.zks.stat(EXTERNAL_CLUSTER_PATH) is None:
      self.zks.create(EXTERNAL_CLUSTER_PATH, "Nodata")

  def clean_zeus(self):
    timeout_secs = 20
    client = SSHClient(host=self.ip, user="nutanix", password="nutanix/4u")
    for path in [EXTERNAL_CLUSTER_PATH, ZEUSCONFIG_CLUSTER_PATH,
                 STATE_CLUSTER_PATH]:
      cluster_list = self.zks.list(path)
      if not cluster_list:
        continue
      for cluster in cluster_list:
        cmd = ("source /etc/profile; zkrm %s/%s" % (path, cluster))
        client.execute(cmd, timeout_secs = timeout_secs)

  def create_pe_entry(self, cluster_id, node_id_list=None):
    self.create_external_cluster_entry(cluster_id)
    self.create_zeusconfig_entry(cluster_id, None, node_id_list)
    self.create_clusterdatastate_entry(cluster_id)

  def create_external_cluster_entry(self, cluster_id):
    config = ConfigurationProto()
    config.logical_timestamp = 1
    if self.zks.get(EXTERNAL_CLUSTER_PATH) is None:
      self.zks.create(EXTERNAL_CLUSTER_PATH, "")
    self.zks.create(EXTERNAL_CLUSTER_PATH + '/' + cluster_id,
                    config.SerializeToString())

  def read_external_cluster_entry(self, cluster_id):
    config = ConfigurationProto()
    cluster_ids = []
    if not cluster_id:
      cluster_ids = self.zks.list(EXTERNAL_CLUSTER_PATH)
    else:
      cluster_ids = [ cluster_id ]

    config_list = {}
    for cluster_id in cluster_ids:
      path = EXTERNAL_CLUSTER_PATH + '/' + cluster_id
      if not self.zks.stat(path):
        log.ERROR('Can not find path %s.' % path)
        config_list.append(None)
        continue
      config_str = self.zks.get(EXTERNAL_CLUSTER_PATH + '/' + cluster_id)
      if not config_str:
        continue
      config.ParseFromString(config_str)
      config_list[cluster_id] = config
    return config_list

  def create_zeusconfig_entry(self, cluster_id, cluster_name=None, node_id_list=None):
    if self.zks.get(ZEUSCONFIG_CLUSTER_PATH) is None:
      self.zks.create(ZEUSCONFIG_CLUSTER_PATH, "")
    path = ZEUSCONFIG_CLUSTER_PATH + '/' + cluster_id
    config = ConfigurationProto()
    text_format.Merge(CONFIG, config)

    config.logical_timestamp = 0
    config.cluster_uuid = cluster_id
    config.cluster_name = (cluster_name or cluster_id or "Unnamed")
    config_str = config.SerializeToString()
    self.zks.create(path, config_str)

  def create_clusterdatastate_entry(self, cluster_id):
    if self.zks.get(STATE_CLUSTER_PATH) is None:
      self.zks.create(STATE_CLUSTER_PATH, "")
    path = STATE_CLUSTER_PATH + '/' + cluster_id
    state_config = ClusterDataState()
    state_config.logical_timestamp = 0
    state_config.cluster_uuid = cluster_id
    config_str = state_config.SerializeToString()
    self.zks.create(path, config_str)

  def read_zeusconfig_entry(self, cluster_uuid):
    config_str = self.zks.get(ZEUSCONFIG_CLUSTER_PATH + "/" + cluster_uuid)
    config = ConfigurationProto()
    config.ParseFromString(config_str)
    return text_format.MessageToString(config)

  def delete(self, cluster_uuid):
    self.zks.delete(EXTERNAL_CLUSTER_PATH + "/" + cluster_uuid)
    self.zks.delete(ZEUSCONFIG_CLUSTER_PATH + "/" + cluster_uuid)
    self.zks.delete(STATE_CLUSTER_PATH + "/" + cluster_uuid)

  def __del__(self):
    self.zks.close()

if __name__=="__main__":
  import sys
  zeus = ZeusUtil(sys.argv[1])
  zeus.clean_zeus()
