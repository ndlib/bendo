#
# Bendo 

class lib_bendo::local() {


        # dternity mount


	file { [ "/mnt/bendo", "/mnt/curatend-batch", "/mnt/curatend-batch/bendo" ]  : 
		ensure => directory,
		mode => 0755,
		owner => "app",
		group => "app",
	} 
}
