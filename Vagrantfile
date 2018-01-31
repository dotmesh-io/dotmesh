# -*- mode: ruby -*-
# vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "cbednarski/ubuntu-1604-large"
  config.vm.network :private_network, :ip => "172.17.1.178"
  config.vm.provider "virtualbox" do |v|
    v.memory = 8192
  end
  config.vm.provision "shell", inline: "bash /vagrant/scripts/install_vagrant.sh"
end