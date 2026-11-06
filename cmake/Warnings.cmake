# distkv_set_warnings(<target>)
# Applies the project warning set to one of our own targets. Third-party code
# is deliberately excluded (it is consumed through SYSTEM include paths).

function(distkv_set_warnings target)
    if(MSVC)
        set(_warnings /W4 /permissive-)
        if(DISTKV_WERROR)
            list(APPEND _warnings /WX)
        endif()
    else()
        set(_warnings
            -Wall
            -Wextra
            -Wpedantic
            -Wshadow
            -Wconversion
            -Wsign-conversion
            -Wnon-virtual-dtor
            -Wold-style-cast
            -Woverloaded-virtual)
        if(DISTKV_WERROR)
            list(APPEND _warnings -Werror)
        endif()
    endif()
    target_compile_options(${target} PRIVATE ${_warnings})
endfunction()
