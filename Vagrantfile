# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrant configuration for DataVault multi-database operational cluster
# Deploys Ubuntu 22.04 with Docker & Docker Compose pre-configured to run
# the 3-node PostgreSQL replication cluster, 3-shard MongoDB cluster, and DataVault App.

Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/jammy64"

  # VM Network & Port Forwarding
  config.vm.network "forwarded_port", guest: 8000, host: 8000, host_ip: "127.0.0.1", id: "web_ui"
  config.vm.network "forwarded_port", guest: 8501, host: 8501, host_ip: "127.0.0.1", id: "streamlit"
  config.vm.network "forwarded_port", guest: 5432, host: 5432, host_ip: "127.0.0.1", id: "pg_primary"
  config.vm.network "forwarded_port", guest: 5433, host: 5433, host_ip: "127.0.0.1", id: "pg_replica1"
  config.vm.network "forwarded_port", guest: 5434, host: 5434, host_ip: "127.0.0.1", id: "pg_replica2"
  config.vm.network "forwarded_port", guest: 5435, host: 5435, host_ip: "127.0.0.1", id: "datavault_db"
  config.vm.network "forwarded_port", guest: 27017, host: 27017, host_ip: "127.0.0.1", id: "mongo_router"

  # VirtualBox Hardware Allocation
  config.vm.provider "virtualbox" do |vb|
    vb.name = "datavault-cluster-vm"
    vb.memory = "4096"
    vb.cpus = 2
  end

  # Provisioning script
  config.vm.provision "shell", inline: <<-SHELL
    set -e
    echo "=========================================================="
    echo "Provisioning DataVault Infrastructure inside Vagrant VM..."
    echo "=========================================================="

    # Update system packages
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y
    apt-get install -y ca-certificates curl gnupg lsb-release

    # Install Docker & Docker Compose Plugin
    mkdir -p /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg --yes
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
      $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

    apt-get update -y
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

    # Enable vagrant user to execute docker commands without sudo
    usermod -aG docker vagrant

    # Ensure scripts have execute permissions
    chmod +x /vagrant/mongo/*.sh 2>/dev/null || true
    chmod +x /vagrant/postgres/replica/*.sh 2>/dev/null || true

    echo "==> Docker and Docker Compose successfully installed."
    echo "To launch the cluster manually inside the VM:"
    echo "  cd /vagrant && docker compose up -d"
  SHELL
end
