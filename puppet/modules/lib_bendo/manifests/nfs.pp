#
# Bendo 

class lib_bendo::nfs() {


        # dternity mount

	# bendo_tape_share  contains fully-qualified  NFS share of Dternity Tape Ingest Cache
        $bendo_tape_share = hiera("bendo_tape_share")

	mount { 'dternity-dir':
		name => '/mnt/bendo',
		device => "${bendo_tape_share}",
		fstype => 'nfs',
		options => "hard,intr,retrans=10,timeo=300,rsize=65536,wsize=1048576,vers=3,proto=tcp,sync",
		remounts => true,
		atboot => true,
		ensure => mounted,
	}

}
