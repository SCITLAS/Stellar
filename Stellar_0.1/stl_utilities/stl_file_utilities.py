# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


#===============================================================================
# 目录对比工具，列出
# 1、A比B多了哪些文件
# 2、B比A多了哪些文件
# 3、二者相同的文件,如果最后修改日期不同，则要标注
# 4、包含子目录
#===============================================================================


import os, time

AFILES = []
BFILES = []
COMMON = []
def getPrettyTime(state):
    return time.strftime('%y-%m-%d %H:%M:%S', time.localtime(state.st_mtime))
def dirCompare(apath, bpath):
    afiles = []
    bfiles = []
    for root, dirs , files in os.walk(apath):
        for f in files :
            afiles.append(root + "\\" + f)
    for root, dirs , files in os.walk(bpath):
        for f in files :
            bfiles.append(root + "\\" + f)
    #===========================================================================
    # 去掉afiles中文件名的apath
    #===========================================================================
    apathlen = len(apath)
    aafiles = []
    for f in afiles:
        aafiles.append(f[apathlen:])
    #===========================================================================
    # 去掉afiles中文件名的apath
    #===========================================================================
    bpathlen = len(bpath)
    bbfiles = []
    for f in bfiles:
        bbfiles.append(f[bpathlen:])
    afiles = aafiles
    bfiles = bbfiles
    setA = set(afiles)
    setB = set(bfiles)
    #===========================================================================
    # 处理共有文件
    #===========================================================================
    commonfiles = setA & setB
    print("#=================================")
    print("common in '", apath, "' and '", bpath, "'")
    print("#=================================")
    print('\t\t\ta:\t\t\t\t\t\tb:')
    for f in sorted(commonfiles):
        print(f + "\t\t" + getPrettyTime(os.stat(apath + "\\" + f)) + "\t\t" + getPrettyTime(os.stat(bpath + "\\" + f)))

    #===========================================================================
    # 处理仅出现在一个目录中的文件
    #===========================================================================
    onlyFiles = setA ^ setB
    aonlyFiles = []
    bonlyFiles = []
    for of in onlyFiles:
        if of in afiles:
            aonlyFiles.append(of)
        elif of in bfiles:
            bonlyFiles.append(of)
    print("#=================================")
    print("#only in ", apath)
    print("#=================================")
    for of in sorted(aonlyFiles):
        print(of)
    print("#=================================")
    print("#only in ", bpath)
    print("#=================================")
    for of in sorted(bonlyFiles):
        print(of)
if __name__ == '__main__':
    dirCompare('C:\liyuhuang\git\Stellar\Stellar_0.1\data\origin\\tushare\security_trade_data\\all', 'C:\liyuhuang\git\Stellar\Stellar_0.1\data\origin\\tushare\security_trade_data\\recent\\5min')