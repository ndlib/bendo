#
# Bendo 

class lib_bendo() {

	# create app user

	include lib_app_home
	include lib_bendo_server

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

        # Batch import CORPFS share

	mount { 'curatend-batchdir':
		name => '/mnt/curatend-batch',
		device => '//library.corpfs.nd.edu/DCNS/Library/Departmental/curatend-batch',
		fstype => 'cifs',
		options => 'rw,nounix,iocharset=utf8,file_mode=0777,dir_mode=0777,credentials=/.creds/wcreds	0	0',
		ensure => mounted,
	}

	# app subdirectory for bendo

	file { '"bendo-appdir": 
		name => "/home/app/bendo",
		ensure => directory,
		mode => 0755,
		owner => "app",
		group => "app",
	} 


	# instantiation order

	Class["lib_app_home"] -> File["bendo-appdir"] -> Class["lib_bendo_server"]

}
