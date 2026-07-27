# apply-patches.cmake — Fault-tolerant patch system for FetchContent deps
#
# Call apply_dep_patches(<dep-name> <source-dir>) between
# FetchContent_Populate() and add_subdirectory() to apply any
# patches from standalone/patches/<dep-name>/*.patch
#
# Behavior:
#   Already applied → skip with status message
#   Applies cleanly → apply and log
#   Doesn't apply   → skip with notice (patch may be obsolete)
#   Never fails the build
#
# Patches are applied in filename order (use NNN- prefix for ordering).

find_program(PATCH_EXECUTABLE patch)

function(apply_dep_patches DEP_NAME SOURCE_DIR)
    if(NOT PATCH_EXECUTABLE)
        message(WARNING "[patch] 'patch' not found, skipping all patches")
        return()
    endif()

    set(_dir "${CMAKE_CURRENT_SOURCE_DIR}/patches/${DEP_NAME}")
    if(NOT IS_DIRECTORY "${_dir}")
        return()
    endif()

    file(GLOB _patches "${_dir}/*.patch")
    if(NOT _patches)
        return()
    endif()
    list(SORT _patches)

    foreach(_patch IN LISTS _patches)
        get_filename_component(_name "${_patch}" NAME)

        # Check if already applied (reverse dry-run succeeds)
        execute_process(
            COMMAND ${PATCH_EXECUTABLE} -p1 -R --dry-run -i "${_patch}"
            WORKING_DIRECTORY "${SOURCE_DIR}"
            RESULT_VARIABLE _rev_rc
            OUTPUT_QUIET ERROR_QUIET
        )
        if(_rev_rc EQUAL 0)
            message(STATUS "[patch] ${DEP_NAME}: ${_name} — already applied")
            continue()
        endif()

        # Check if it applies cleanly (forward dry-run)
        execute_process(
            COMMAND ${PATCH_EXECUTABLE} -p1 --dry-run -i "${_patch}"
            WORKING_DIRECTORY "${SOURCE_DIR}"
            RESULT_VARIABLE _fwd_rc
            OUTPUT_QUIET ERROR_QUIET
        )
        if(_fwd_rc EQUAL 0)
            execute_process(
                COMMAND ${PATCH_EXECUTABLE} -p1 -i "${_patch}"
                WORKING_DIRECTORY "${SOURCE_DIR}"
                RESULT_VARIABLE _apply_rc
            )
            if(_apply_rc EQUAL 0)
                message(STATUS "[patch] ${DEP_NAME}: ${_name} — applied")
            else()
                message(WARNING "[patch] ${DEP_NAME}: ${_name} — apply FAILED")
            endif()
        else()
            message(STATUS
                "[patch] ${DEP_NAME}: ${_name} — skipped (no longer applies)")
        endif()
    endforeach()
endfunction()
