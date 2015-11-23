#
# Bendo 

class lib_bendo::nfs() {


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

}
