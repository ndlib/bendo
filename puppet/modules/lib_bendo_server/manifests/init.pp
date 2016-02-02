
# Application roles class for Bendo
#
# Assumes that bendo_root exists

class lib_bendo_server( $bendo_root = '/opt/bendo', $branch='master') {

include lib_runit

$goroot = "${bendo_root}/gocode"
$target = 'github.com/ndlib/bendo/cmd/bendo'
$repo = "github.com/ndlib/bendo"

# hiera config for runit run scripts

$bendo_cache_dir = hiera("bendo_cache_dir")
$bendo_storage_dir = hiera("bendo_storage_dir")

# Go Packages -  refactor into lib_go?

	$pkglist = [
		"golang",
		"jq"
	]

	package { $pkglist:
		ensure => present,
	}

# Create Logdir. Runit will manage logrotate

	file { "bendo-logdir":
		name =>  "${bendo_root}/log",
		ensure => directory,
	}


# Build and intall Go code from the repo

	class { 'lib_go::build':
		goroot => $goroot,
		branch => $branch,
		target => $target,
		repo => $repo,
		require => Package[$pkglist],
		notify => Service['bendo'],
	} 

# Create bendo runit service directories

	$bendorunitdirs = [ "/etc/sv/bendo", "/etc/sv/bendo/log" ]

	file { $bendorunitdirs:
		ensure => directory,
		owner => "app",
		group => "app",
		require => Class[['lib_runit','lib_go::build']],
	} 

# make exec and log files for runit

	file { 'bendorunitexec':
		name => '/etc/sv/bendo/run',
		owner => "app",
		group => "app",
		mode => '0755',
		replace => true,
		content => template('lib_bendo_server/bendo.exec.erb'),
                require => File[$bendorunitdirs],
	} 


	file { 'bendorunitlog':
		name => '/etc/sv/bendo/log/run',
		owner => "app",
		group => "app",
		replace => true,
		mode => '0755',
		content => template('lib_bendo_server/bendo.log.erb'),
                require => File['bendorunitexec'],
	}

# Enable the Service ( leave this out until app can run /sbin/sv ) 

#	service { 'bendo':
#		provider => 'base',
#		ensure => running,
#		enable => true,
#		hasstatus => false,
#		hasrestart => false,
#		restart => '/sbin/sv restart bendo',
#		start => '/sbin/sv start bendo',
#		stop => '/sbin/sv stop bendo',
#		status => '/sbin/sv status bendo',
#		require => File['bendorunitlog'],
#	}

}
