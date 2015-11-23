# Dev Machine on Vagrant,
# using Bendo staging provisoning 

include lib_hiera

# Add missing CentOS Classes

$packagelist = [ 'git', 'unzip', 'epel-release']

package { $packagelist:
	ensure => 'installed',
        before => Group["app"],
}

# Need app user, group

group { 'app':
	ensure => present,
	gid => 1518,
	before => User['app'],
} 

user { 'app':
	ensure => present,
	uid => 1518,
	gid => 1518,
	before => Class['lib_bendo'],
}

# OK - build the application!

class { 'lib_bendo':
      require => Package[$packagelist],
}



