# Function to configure a Proteus experiment target.
#
# This function sets up the necessary configurations for a given target
# to be used as a Proteus experiment. It enables default warnings for the
# target and includes the _proteus-install module to handle installation
# specifics.
#
# Arguments:
#   target_name - The name of the target to configure.
#
# Example usage:
#   configure_proteus_experiment(my_target)
#
# Note:
#   This function should be called after the target has been defined.
#   _proteus-install should also be converted to a function
function(configure_proteus_experiment target_name)
  set(_proteus_install_target ${target_name})

  # set(_proteus_install_dev ${target_name}_Development)
  # set(_proteus_install_bin ${target_name}_Experiments)
  # set(_proteus_install_dev ${target_name}_${PROTEUS_CPACK_COMP_SUFFIX_DEV})
  # set(_proteus_install_bin ${target_name}_${PROTEUS_CPACK_COMP_SUFFIX_EXPERIMENTS})

  target_enable_default_warnings(${target_name})

  set(_proteus_install_dev ${PROTEUS_CPACK_COMP_DEV})
  set(_proteus_install_bin ${PROTEUS_CPACK_COMP_EXPERIMENTS})
  include(_proteus-install)
endfunction()
