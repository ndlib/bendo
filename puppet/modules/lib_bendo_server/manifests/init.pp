
# Application roles class for Bendo
#
# Assumes that bendo_root exists

class lib_bendo_server( $bendo_root = '/opt/bendo', $branch='master') {

$goroot = "${bendo_root}/gocode"
$target = 'github.com/ndlib/bendo/cmd/bendo'
$repo = "github.com/ndlib/bendo"

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
	} 

# Create bendo runit service directories

	$bendorunitdirs = [ "/etc/sv", "/etc/sv/bendo", "/etc/sv/bendo/log" ]

	file { $bendorunitdirs:
		ensure => directory,
		owner => "app",
		group => "app",
		require => Class['lib_go::build'],
	} ->

# make exec and log files for runit

	file { 'bendorunitexec':
		name => '/etc/sv/bendo/run',
		replace => true,
		content => template('lib_bendo_server/bendo.exec.erb'),
	} ->


	file { 'bendorunitlog':
		name => '/etc/sv/bendo/log/run',
		replace => true,
		content => template('lib_bendo_server/bendo.log.erb'),
	} ->

# Enable the Service

	service { 'bendo':
		provider => "runit",
		ensure => running,
		enable => true,
	}

}
