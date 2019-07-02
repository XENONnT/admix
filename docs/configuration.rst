=============
Configuration
=============

The main purpose of aDMIX is to take over the interaction with the data management tool Rucio (source) what is provided by ATLAS. Therefore you need to setup aDMIX with several configuration files which present the data outline of your experiment on local disks and the later data naming convention in Rucio with containers, datasets and files.

This part of the documentation guides you through the necessary configuration.

aDMIX basic configuration
-------------------------
The basic configuration from an example configuration file is given here in /admix/config/host_config_dummy.config

.. literalinclude:: ../admix/config/host_config_dummy.config

To begin with you need to setup certain keys beforehand:

  * host: A short abbreviation of hostname to which individual database data locations refer later.
  
    The hostname of your data facility is data-host1.cluster.aws.com. To manage your data locations later in a database or simplicity, you will use an abbreviation for the long hostname (e.g. data-host1):
    
    data-host1:
      - /data/path/to/dataset/dataset_01
      - /data/path/to/dataset/dataset_02
      - /data/path/to/dataset/dataset_03
    
  * hostname: The hostname of your data facility. For practial reasons this should be same name such it is used in your HOSTNAME bash variable.
  * log_path: Specify the path to your log file. All logs will go into one log file. Log file rotation is not yet supported.
  