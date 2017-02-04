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
}
