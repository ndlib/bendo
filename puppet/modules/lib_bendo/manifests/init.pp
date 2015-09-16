#
# Bendo 

class lib_bendo( $bendo_root="/opt/bendo", $branch='master') {


        # dternity mount

	mount { 'dternity-dir':
		name => '/mnt/bendo',
		device => '192.168.49.104:/shares/library_CurateND_staging',
		fstype => 'nfs',
		options => "hard,intr,retrans=10,timeo=300,rsize=65536,wsize=1048576,vers=3,proto=tcp,sync",
		remounts => true,
		atboot => true,
		ensure => mounted,
	}


	# app subdirectory for bendo
        $bendo_dirs = [ "/home/app/bendo", $bendo_root ]

	file { $bendo_dirs : 
		ensure => directory,
		mode => 0755,
		owner => "app",
		group => "app",
		require =>  Mount['dternity-dir']
	} 

	# Instantiate Application Class

	class { 'lib_bendo_server': 
		bendo_root => $bendo_root,
		branch => $branch,
		require => File[ $bendo_dirs ],
	}
		





}
