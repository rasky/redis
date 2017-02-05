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
        set res
    } {1 1 1 1 1 1 1 1}
}
