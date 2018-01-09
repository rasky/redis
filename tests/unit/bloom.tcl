start_server {tags {"bloom"}} {
    # test {BloomFilter self test passes} {
    #     catch {r bfselftest} e
    #     set e
    # } {OK}

    test {BFADD without arguments creates a value} {
        r bfadd bloom
        r exists bloom
    } {1}

    test {BFADD cannot change error on default} {
        catch {r bfadd bloom error 0.1} e
        set e
    } {*ERR*}

    test {BFADD can set error} {
        r del bloom
        r bfadd bloom error 0.1
        r bfadd bloom error 0.1
        catch {r bfadd bloom error 0.2} e
        set e
    } {*ERR*}

    test {Bloom adding elements and checking} {
        r del bloom
        r bfadd bloom elements a b c d e
        r bfadd bloom elements f g h i j
        r bfadd bloom elements k l m n o
        set res {}
        lappend res [r bfexist bloom a]
        lappend res [r bfexist bloom b]
        lappend res [r bfexist bloom c]
        lappend res [r bfexist bloom d]
        lappend res [r bfexist bloom l]
        lappend res [r bfexist bloom m]
        lappend res [r bfexist bloom n]
        lappend res [r bfexist bloom o]
        lappend res [r bfexist bloom z]
        set res
    } {1 1 1 1 1 1 1 1 0}

    test {Bloom adding elements return check} {
        r del bloom
        set res {}
        lappend res [r bfadd bloom elements a b c d a]
        lappend res [r bfadd bloom elements a b e f c]
        lappend res [r bfadd bloom elements z z z z z]
        lappend res [r bfadd bloom elements a c z e c]
        lappend res [r bfadd bloom elements k a a a a]
        set res
    } {4 2 1 0 1}

    test {Bloom cardinality} {
        set res {}
        r del bloom
        r bfadd bloom elements a b c d e
        lappend res [r bfcount bloom]
        r bfadd bloom elements f g h i j
        lappend res [r bfcount bloom]
        r bfadd bloom elements k l m n o
        lappend res [r bfcount bloom]
        r bfadd bloom elements a g h z k
        lappend res [r bfcount bloom]
        set res
    } {5 10 15 16}

    test {Bloom testing different error rate} {

        foreach error {0.1 0.01 0.001} {
            r del bloom
            r bfadd bloom error $error

            set n 0
            set checks {}
            while {$n < 500000} {
                set elements {}
                for {set j 0} {$j < 100} {incr j} {lappend elements [expr rand()]}
                lappend checks [lindex $elements 0]
                lappend checks [expr rand()]
                incr n 100
                r bfadd bloom elements {*}$elements

                set card [r bfcount bloom]
                assert {abs($card - $n) / $n < 0.005}
            }

            set total [llength $checks]
            set errors 0
            foreach {good bad} $checks {
                set res [r bfexist bloom $good]
                assert {$res == 1}

                set res [r bfexist bloom $bad]
                if {$res == 1} {
                    incr errors 1
                }
            }

            # puts "DEBUG"
            # puts [r bfdebug status bloom]
            # puts [r bfdebug filter bloom 0]
            # puts [r bfdebug filter bloom 1]
            # puts [r bfdebug filter bloom 2]
            # puts [r bfdebug filter bloom 3]
            # puts [r bfdebug filter bloom 4]
            # puts [r bfdebug filter bloom 5]

            set realerror [expr {double($errors) / double($total)}]
            # puts $errors
            # puts $total
            # puts $error
            # puts $realerror
            assert {$realerror <= $error}
        }
    }
}


