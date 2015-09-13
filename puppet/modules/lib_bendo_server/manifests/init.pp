class lib_bendo_server( $bendo_root = '/opt/bendo') {

	$pkglist = [
		"golang",
		"jq"
	]

	package { $pkglist:
		ensure => present,
	}

	file { "bendo-logdir":
		name => [ "$bendo_root", "${bendo_root}/log" ]
		ensure => directory,
	}

	exec { "Build-bendo-from-repo":
		command => "/bin/bash -c \"export GOPATH=${bendo_root} && go get -u github.com/ndlib/curatend-batch\"",
		require => File[$bendo_root],
	}

	file { 'bendo.conf':
		name => '/etc/init/bendo.conf',
		replace => true,
		content => template('lib_bendo/upstart.erb'),
		require => Exec["Build-bendo-from-repo"],
	}

	file { 'bendo/tasks':
		name => "${bendo_root}/tasks",
		ensure => 'directory',
		source => "${bendo_root}/src/github.com/ndlib/curatend-batch/tasks",
		recurse => true,
		purge => true,
		require => Exec['Build-bendo-from-repo'],
	}

	file { 'bendo/tasks/conf':
		name => "${bendo_root}/tasks/conf",
		replace => true,
		content => template('lib_bendo/tasks.conf.erb'),
		require => File['bendo/tasks'],
	}

	file { 'logrotate.d/bendo':
		name => '/etc/logrotate.d/bendo',
		replace => true,
		require => File["bendo/tasks/conf"],
		content => template('lib_bendo/logrotate.erb'),
	}

	exec { "stop-bendo":
		command => "/sbin/initctl stop bendo",
		unless => "/sbin/initctl status bendo | grep stop",
		require => File['logrotate.d/bendo'],
	}

	exec { "start-bendo":
		command => "/sbin/initctl start bendo",
		unless => "/sbin/initctl status bendo | grep process",
		require => Exec["stop-bendo"]
	}

        Package[$pkglist] -> File["bendo-logdir"] 
}
