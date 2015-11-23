#
# Bendo 

class lib_bendo( $bendo_root="/opt/bendo", $branch='master') {

include lib_app_home


        # dternity mount
        #

	$bendo_mount_type = hiera('bendo_mount_type')


	class { "lib_bendo::${bendo_mount_type}": }

	# app subdirectory for bendo
        $bendo_dirs = [ "/home/app/bendo", $bendo_root ]

	file { $bendo_dirs : 
		ensure => directory,
		mode => 0755,
		owner => "app",
		group => "app",
		require =>  Class[[ "lib_app_home", "lib_bendo::${bendo_mount_type}"]]
	} 

	# Instantiate Application Class

	class { 'lib_bendo_server': 
		bendo_root => $bendo_root,
		branch => $branch,
		require => [ Class["lib_bendo::${bendo_mount_type}"], File[ $bendo_dirs ]],
	}
		





}
