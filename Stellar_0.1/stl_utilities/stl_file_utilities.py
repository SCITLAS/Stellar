# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import os, time


'''
比较两个文件夹中文件的差异, 从网上搬得: http://blog.csdn.net/imzoer/article/details/8675078
谢谢原作者
'''
AFILES = []
BFILES = []
COMMON = []
def dirCompare(apath, bpath):
    #===============================================================================
    # 目录对比工具，列出
    # 1、A比B多了哪些文件
    # 2、B比A多了哪些文件
    # 3、二者相同的文件,如果最后修改日期不同，则要标注
    # 4、包含子目录
    #===============================================================================
    afiles = []
    bfiles = []
    for root, dirs , files in os.walk(apath):
        for f in files :
            afiles.append(root + "/" + f)
    for root, dirs , files in os.walk(bpath):
        for f in files :
            bfiles.append(root + "/" + f)
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
        print(f + "\t\t" + getPrettyTime(os.stat(apath + "/" + f)) + "\t\t" + getPrettyTime(os.stat(bpath + "/" + f)))

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

def getPrettyTime(state):
    return time.strftime('%y-%m-%d %H:%M:%S', time.localtime(state.st_mtime))


'''
获取指定路径文件夹下面所有的文件列表, 从网上搬得: http://blog.csdn.net/rumswell/article/details/9818001
感谢原作者
'''
def get_file_fist(find_path, flag_str=[]):
    '''''
    #获取目录中指定的文件名
    #>>>flag_str=['F','EMS','txt'] #要求文件名称中包含这些字符
    #>>>file_list=get_file_fist(find_path, flag_str) #
    '''
    file_list = []
    file_names = os.listdir(find_path)
    if (len(file_names) > 0):
        for fn in file_names:
            if (len(flag_str) > 0):
                #返回指定类型的文件名
                if (is_sub_string(flag_str, fn)):
                    full_file_name = os.path.join(find_path, fn)
                    file_list.append(full_file_name)
            else:
                #默认直接返回所有文件名
                full_file_name = os.path.join(find_path,fn)
                file_list.append(full_file_name)

    #对文件名排序
    if (len(file_list) > 0):
        file_list.sort()

    return file_list

def is_sub_string(sub_strList, str):
    '''''
    #判断字符串Str是否包含序列SubStrList中的每一个子字符串
    #>>>sub_str_list=['F','EMS','txt']
    #>>>str='F06925EMS91.txt'
    #>>>is_sub_string(sub_str_list, str)#return True (or False)
    '''
    flag=True
    for sub_str in sub_strList:
        if not(sub_str in str):
            flag=False

    return flag

def get_code_list_in_dir(data_path):
    '''
    获取data_path对应的文件夹中所有csv文件名所代表的code

    Parameters
    ------
        data_path: 指定文件夹路径
    return
    -------
        code_list: code列表
    '''
    code_list = []
    file_names = os.listdir(data_path)
    if (len(file_names) > 0):
        for fn in file_names:
            if (is_sub_string(['csv'], fn)):
                tmp_list = fn.split('.')
                code_list.append(tmp_list[0])

    return code_list


if __name__ == '__main__':
    # dirCompare('../data/origin/tushare/security_trade_data/all', '../data/origin/tushare/security_trade_data/recent/5min')

    dir_path = '../data/origin/tushare/security_trade_data/all'
    # file_list = get_file_fist(dir_path, flag_str=['csv'])
    # print(len(file_list))
    # print(file_list)

    code_list = get_code_list_in_dir(dir_path)
    print(code_list)
    print(len(code_list))