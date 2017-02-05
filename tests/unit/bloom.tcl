start_server {tags {"hll"}} {
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

    test {Bloom testing different error rate} {

        foreach error {0.1 0.01 0.001} {
            r del bloom
            r bfadd bloom error $error

            set n 0
            set checks {}
            while {$n < 100000} {
                set elements {}
                for {set j 0} {$j < 100} {incr j} {lappend elements [expr rand()]}
                lappend checks [lindex $elements 0]
                lappend checks [expr rand()]
                incr n 100
                r bfadd bloom elements {*}$elements
            }

            set total [llength $checks]
            set errors 0
            foreach {good bad} $checks {
                set res [r bfexist bloom $good]
                if {$res == 0} {
                    incr errors 1
                }

                set res [r bfexist bloom $bad]
                if {$res == 1} {
                    incr errors 1
                }
            }

            set realerror [expr {double($errors) / double($total)}]
            assert {$realerror > ($error/5.0)}
            assert {$realerror < ($error*5.0)}
        }
    }
}


