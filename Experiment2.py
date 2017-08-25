import numpy as np  
import pandas as pd 
from os import listdir
import multiprocessing
from multiprocessing import Pool
import copy
from functools import partial

def parallelized_function(entity2id_dict,relation2id_dict,df):
	for i in range(df.shape[0]):
		df.iloc[i,0]=entity2id_dict[df.iloc[i,0]]
		df.iloc[i,1]=relation2id_dict[df.iloc[i,1]]
		df.iloc[i,2]=entity2id_dict[df.iloc[i,2]]
		# df.iloc[i,0]="YOLO"
		# print entity2id_dict
		# print relation2id_dict
	return df

# Get the joint dataset of for URI,URI,URI,language
all_files=listdir('temp/URI_4_cols/')
only_bz2=[x for x in all_files if x.endswith('bz2')]

# Create empty dataframe and append to it!
uri=pd.DataFrame()
for filename in only_bz2:
	df=pd.read_table('temp/URI_4_cols/'+filename,sep="\t",header=None,compression="bz2",names=["Subject","Predicate","Object","Language"])
	if df.empty==False:
		uri=uri.append(df)

# uri=pd.read_csv('files.txt',sep="\t",header=None,names=["s","p","o","Language"])

# Calculate total number of triples of each language
uri=uri.sort_values(by="Language")
es_triples=sum(uri.iloc[:,3]=="es")
ja_triples=sum(uri.iloc[:,3]=="ja")
nl_triples=sum(uri.iloc[:,3]=="nl")

# Break up data into different languages
spanish=uri[0:es_triples]
japanese=uri[es_triples:es_triples+ja_triples]
dutch=uri[es_triples+ja_triples:es_triples+ja_triples+nl_triples]

# Jumble up the data
spanish=spanish.sample(frac=1).reset_index(drop=True)
japanese=japanese.sample(frac=1).reset_index(drop=True)
dutch=dutch.sample(frac=1).reset_index(drop=True)

# Break data into 80-20% for train-test split
spanish_80=spanish[0:int(0.8*es_triples)]
spanish_20=spanish[int(0.8*es_triples):es_triples]
japanese_80=japanese[0:int(0.8*ja_triples)]
japanese_20=japanese[int(0.8*ja_triples):ja_triples]
dutch_80=dutch[0:int(0.8*nl_triples)]
dutch_20=dutch[int(0.8*nl_triples):nl_triples]

# Joint train and test of form URI,URI,URI.Language column removed.
joint_train=pd.concat([spanish_80,japanese_80,dutch_80])
joint_test=pd.concat([spanish_20,japanese_20,dutch_20])
joint_train=joint_train.iloc[:,0:3]
joint_test=joint_test.iloc[:,0:3]

# Get the entity2id mappings
all_files=listdir('temp/Entity2id/')
only_bz2=[x for x in all_files if x.endswith('bz2')]

# Create empty dataframe and append to it!
entity2id=pd.DataFrame()
for filename in only_bz2:
	df=pd.read_table('temp/Entity2id/'+filename,sep="\t",header=None,compression="bz2",names=["Entity","Id"])
	if df.empty==False:
		entity2id=entity2id.append(df)

# entity2id=pd.read_csv('e2id.txt',sep="\t",header=None,names=["Entity","Id"])
entity2id=entity2id.sort_values(by="Id")
# Convert the dataframe to a dictionary
entity2id_dict=entity2id.set_index("Entity")["Id"].to_dict()



# Get the relation2id mappings
all_files=listdir('temp/Rel2id/')
only_bz2=[x for x in all_files if x.endswith('bz2')]

# Create empty dataframe and append to it!
relation2id=pd.DataFrame()
for filename in only_bz2:
	df=pd.read_table('temp/Rel2id/'+filename,sep="\t",header=None,compression="bz2",names=["Relation","Id"])
	if df.empty==False:
		relation2id=relation2id.append(df)

# relation2id=pd.read_csv('r2id.txt',sep="\t",header=None,names=["Relation","Id"])
relation2id=relation2id.sort_values(by="Id")
# Convert the dataframe to a dictionary
relation2id_dict=relation2id.set_index("Relation")["Id"].to_dict()

# Creating copy variables
joint_train_ex1=copy.copy(joint_train)
joint_test_ex1=copy.copy(joint_test)
joint_train_ex2=copy.copy(joint_train)
joint_test_ex2=copy.copy(joint_test)


num_processes=multiprocessing.cpu_count()
# Mappings the train set to ID format for experiment 1!
df_split=np.array_split(joint_train_ex1,num_processes)
pool=Pool(num_processes)
func=partial(parallelized_function,entity2id_dict,relation2id_dict)
joint_train_ex1=pd.concat(pool.map(func,df_split))
pool.close()
pool.join()

# Mappings the test set to ID format for experiment 1!
df_split=np.array_split(joint_test_ex1,num_processes)
pool=Pool(num_processes)
func=partial(parallelized_function,entity2id_dict,relation2id_dict)
joint_test_ex1=pd.concat(pool.map(func,df_split))
pool.close()
pool.join()

# Mappings the train set to ID format for experiment 1!
for i in range(joint_train_ex1.shape[0]):
	joint_train_ex1.iloc[i,0]=entity2id_dict[joint_train_ex1.iloc[i,0]]
	joint_train_ex1.iloc[i,1]=relation2id_dict[joint_train_ex1.iloc[i,1]]
	joint_train_ex1.iloc[i,2]=entity2id_dict[joint_train_ex1.iloc[i,2]]

# Mappings the test set to ID format for experiment 1!
for i in range(joint_test_ex1.shape[0]):
	joint_test_ex1.iloc[i,0]=entity2id_dict[joint_test_ex1.iloc[i,0]]
	joint_test_ex1.iloc[i,1]=relation2id_dict[joint_test_ex1.iloc[i,1]]
	joint_test_ex1.iloc[i,2]=entity2id_dict[joint_test_ex1.iloc[i,2]]


# Now,read in the sameAs links dataset and modify the entity2id based on that!
all_files=listdir('temp/URI/')
only_bz2=[x for x in all_files if x.endswith('bz2')]

sameAs=pd.DataFrame()
for filename in only_bz2:
	df=pd.read_table('temp/URI/'+filename,sep="\t",header=None,compression="bz2",names=["Subject","Predicate","Object"])
	if df.empty==False:
		sameAs=sameAs.append(df)

modified_entity2id_dict=copy.copy(entity2id_dict)

# Working on the same as links!
for i in range(sameAs.shape[0]):
	if(sameAs.iloc[i,0] in modified_entity2id_dict)&(sameAs.iloc[i,2] in modified_entity2id_dict):
		modified_entity2id_dict[sameAs.iloc[i,0]]=min(modified_entity2id_dict[sameAs.iloc[i,0]],modified_entity2id_dict[sameAs.iloc[i,2]])
		modified_entity2id_dict[sameAs.iloc[i,2]]=min(modified_entity2id_dict[sameAs.iloc[i,0]],modified_entity2id_dict[sameAs.iloc[i,2]])


# Mappings the train set to ID format for experiment 2!
df_split=np.array_split(joint_train_ex2,num_processes)
pool=Pool(num_processes)
func=partial(parallelized_function,modified_entity2id_df,relation2id_dict)
joint_train_ex2=pd.concat(pool.map(func,df_split))
pool.close()
pool.join()

# Mappings the test set to ID format for experiment 2!
df_split=np.array_split(joint_test_ex2,num_processes)
pool=Pool(num_processes)
func=partial(parallelized_function,modified_entity2id_df,relation2id_dict)
joint_test_ex2=pd.concat(pool.map(func,df_split))
pool.close()
pool.join()

# Mappings the train set to ID format for experiment 2!
for i in range(joint_train_ex2.shape[0]):
	joint_train_ex2.iloc[i,0]=modified_entity2id_dict[joint_train_ex2.iloc[i,0]]
	joint_train_ex2.iloc[i,1]=relation2id_dict[joint_train_ex2.iloc[i,1]]
	joint_train_ex2.iloc[i,2]=modified_entity2id_dict[joint_train_ex2.iloc[i,2]]

# Mappings the test set to ID format for experiment 2!
for i in range(joint_test_ex2.shape[0]):
	joint_test_ex2.iloc[i,0]=modified_entity2id_dict[joint_test_ex2.iloc[i,0]]
	joint_test_ex2.iloc[i,1]=relation2id_dict[joint_test_ex2.iloc[i,1]]
	joint_test_ex2.iloc[i,2]=modified_entity2id_dict[joint_test_ex2.iloc[i,2]]

# Converting modified entity2id dictionary to a data frame so that can be saved
modified_entity2id_df=pd.DataFrame(modified_entity2id_dict.items(),columns=["Entity","Id"])
modified_entity2id_df=modified_entity2id_df.sort_values(by="Id")


# Saving all files!
entity2id.to_csv('temp/entity2id.txt',header=False,index=False,sep="\t",mode="w+")
relation2id.to_csv('temp/relation2id.txt',header=False,index=False,sep="\t",mode="w+")
joint_train.to_csv('temp/URI_joint_train.txt',header=False,index=False,sep="\t",mode="w+")
joint_test.to_csv('temp/URI_joint_test.txt',header=False,index=False,sep="\t",mode="w+")
joint_train_ex1.to_csv('temp/triple2id_ex1.txt',header=False,index=False,sep="\t",mode="w+")
joint_test_ex1.to_csv('temp/test2id_ex1.txt',header=False,index=False,sep="\t",mode="w+")
joint_train_ex2.to_csv('temp/triple2id_ex2.txt',header=False,index=False,sep="\t",mode="w+")
joint_test_ex2.to_csv('temp/test2id_ex2.txt',header=False,index=False,sep="\t",mode="w+")
modified_entity2id_df.to_csv('temp/modified_entity2id.txt',header=False,index=False,sep="\t",mode="w+")

# Adding the number of lines of triples train file experiment 1
f=open('temp/triple2id_ex1.txt','r')
temp=f.read()
f.close()
f=open('temp/triple2id_ex1.txt','w')
f.write(str(joint_train_ex1.shape[0])+"\n"+temp)
f.close()

# Adding number of lines to entity2id file
f=open('temp/entity2id.txt','r')
temp=f.read()
f.close()
f=open('temp/entity2id.txt','w')
f.write(str(entity2id.shape[0])+"\n"+temp)
f.close()

#  Adding number of lines to relation2id file
f=open('temp/relation2id.txt','r')
temp=f.read()
f.close()
f=open('temp/relation2id.txt','w')
f.write(str(relation2id.shape[0])+"\n"+temp)
f.close()

# Adding the number of lines of triples train file experiment 2
f=open('temp/triple2id_ex2.txt','r')
temp=f.read()
f.close()
f=open('temp/triple2id_ex2.txt','w')
f.write(str(joint_train_ex2.shape[0])+"\n"+temp)
f.close()

# Adding number of lines to modified entity2id file
f=open('temp/modified_entity2id.txt','r')
temp=f.read()
f.close()
f=open('temp/modified_entity2id.txt','w')
f.write(str(modified_entity2id_df.shape[0])+"\n"+temp)
f.close()