import numpy as np  
import pandas as pd 
from os import listdir
import multiprocessing
from multiprocessing import Pool,Lock
import copy
from functools import partial
import itertools

def parallelized_function(entity2id_dict,relation2id_dict,df):
	for i in range(df.shape[0]):
		df.iloc[i,0]=entity2id_dict[df.iloc[i,0]]
		df.iloc[i,1]=relation2id_dict[df.iloc[i,1]]
		df.iloc[i,2]=entity2id_dict[df.iloc[i,2]]
		# df.iloc[i,0]=temp.iloc[0,0]
		# print entity2id_dict
		# print relation2id_dict
	return df

def sameAs_adder(entity2id_dict,relation2id_dict,sameas):
	sameas_ids=pd.DataFrame(columns=["Subject","Predicate","Object"])
	for i in range(sameas.shape[0]):
		if(sameas.iloc[i,0] in entity2id_dict)&(sameas.iloc[i,2] in entity2id_dict):
			sameas_ids.loc[sameas_ids.shape[0]]=[entity2id_dict[sameas.iloc[i,0]],sameAs_relation_id,entity2id_dict[sameas.iloc[i,2]]]

	return sameas_ids


# Reading in the entity2id files
print "Reading entity2id"
all_files=listdir('temp/Entity2id/')
only_bz2=[x for x in all_files if x.endswith('bz2')]

entity2id=pd.DataFrame()
for filename in only_bz2:
	df=pd.read_table('temp/Entity2id/'+filename,sep="\t",header=None,compression="bz2",names=["Entity","Id"])
	if df.empty==False:
		entity2id=entity2id.append(df)

# entity2id=pd.read_csv('e2id.txt',sep="\t",header=None,names=["Entity","Id"])
entity2id=entity2id.sort_values(by="Id").reset_index(drop=True)
# Converting to a dictionary
entity2id_dict=entity2id.set_index("Entity")["Id"].to_dict()
print "Saving entity2id"
entity2id.to_csv('temp/entity2id.txt',header=False,index=False,sep="\t",mode="w+")

print "Reading relation2id"
# Reading in the relation2id files
all_files=listdir('temp/Rel2id/')
only_bz2=[x for x in all_files if x.endswith('bz2')]

relation2id=pd.DataFrame()
for filename in only_bz2:
	df=pd.read_table('temp/Rel2id/'+filename,sep="\t",header=None,compression="bz2",names=["Relation","Id"])
	if df.empty==False:
		relation2id=relation2id.append(df)

# relation2id=pd.read_csv('r2id.txt',sep="\t",header=None,names=["Relation","Id"])
relation2id=relation2id.sort_values(by="Id").reset_index(drop=True)

print "Reading sameAs"
# Now,read in the sameAs links dataset and modify the entity2id based on that!
all_files=listdir('temp/URI/')
only_bz2=[x for x in all_files if x.endswith('bz2')]

sameAs=pd.DataFrame()
for filename in only_bz2:
	df=pd.read_table('temp/URI/'+filename,sep="\t",header=None,compression="bz2",names=["Subject","Predicate","Object"])
	if df.empty==False:
		sameAs=sameAs.append(df)
sameAs=sameAs.sort_values(by="Subject").reset_index(drop=True)
# sameAs=pd.read_csv('sameas.txt',sep="\t",header=None,names=["s","p","o"])

print "Adding sameAs relation to the relation2id"
# Add the owl:sameAs link with a new Id in the end 
sameAs_relation_id=relation2id.shape[0]
relation2id.loc[relation2id.shape[0]]=[sameAs.iloc[0,1],relation2id.shape[0]]
# Convert it to a dictionary
relation2id_dict=relation2id.set_index("Relation")["Id"].to_dict()
print "Saving relation2id"
relation2id.to_csv('temp/relation2id.txt',header=False,index=False,sep="\t",mode="w+")
print "Saving sameAs triples URI file"
sameAs.to_csv('temp/sameAs_URI.txt',header=False,index=False,sep="\t",mode="w+")


print "Getting the joint dataset"
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
uri=uri.sort_values(by="Language").reset_index(drop=True)
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

print "Breaking in train-validation-test split"
# Break data into 80-20% for train-test split
spanish_train=spanish[0:int(0.7*es_triples)].reset_index(drop=True)
spanish_valid=spanish[int(0.7*es_triples):int(0.85*es_triples)].reset_index(drop=True)
spanish_test=spanish[int(0.85*es_triples):es_triples].reset_index(drop=True)
japanese_train=japanese[0:int(0.7*ja_triples)].reset_index(drop=True)
japanese_valid=japanese[int(0.7*ja_triples):int(0.85*ja_triples)].reset_index(drop=True)
japanese_test=japanese[int(0.85*ja_triples):ja_triples].reset_index(drop=True)
dutch_train=dutch[0:int(0.7*nl_triples)].reset_index(drop=True)
dutch_valid=dutch[int(0.7*nl_triples):int(0.85*nl_triples)].reset_index(drop=True)
dutch_test=dutch[int(0.85*nl_triples):nl_triples].reset_index(drop=True)

# Joint train and test of form URI,URI,URI.Language column removed.
joint_train=pd.concat([spanish_train,japanese_train,dutch_train]).reset_index(drop=True)
joint_valid=pd.concat([spanish_valid,japanese_valid,dutch_valid]).reset_index(drop=True)
joint_test=pd.concat([spanish_test,japanese_test,dutch_test]).reset_index(drop=True)
joint_train=joint_train.iloc[:,0:3]
joint_valid=joint_valid.iloc[:,0:3]
joint_test=joint_test.iloc[:,0:3]

print "Saving joint train and test in URI forms"
joint_train.to_csv('temp/URI_joint_train.txt',header=False,index=False,sep="\t",mode="w+")
joint_valid.to_csv('temp/URI_joint_valid.txt',header=False,index=False,sep="\t",mode="w+")
joint_test.to_csv('temp/URI_joint_test.txt',header=False,index=False,sep="\t",mode="w+")

# Creating copy variables
joint_train_ex1=copy.copy(joint_train)
joint_test_ex1=copy.copy(joint_test)
joint_valid_ex1=copy.copy(joint_valid)
joint_train_ex2=copy.copy(joint_train)
joint_test_ex2=copy.copy(joint_test)
joint_valid_ex2=copy.copy(joint_valid)

print "Saving spanish dataset in URI format"
# Saving spanish_80 and spanish_20 in URI format so they can be mapped by spanish mappings later for the required task! 
spanish_train.to_csv('temp/Spanish_Train_URI.txt',header=False,index=False,sep="\t",mode="w+")
spanish_valid.to_csv('temp/Spanish_Valid_URI.txt',header=False,index=False,sep="\t",mode="w+")
spanish_test.to_csv('temp/Spanish_Test_URI.txt',header=False,index=False,sep="\t",mode="w+")

print "Mapping for experiment 1 data train"
# Mapping the experiment train and test to ID. Will add sameAs links after this.
num_processes=multiprocessing.cpu_count()
# Mappings the train set to ID format for experiment 1!
df_split=np.array_split(joint_train_ex1,num_processes)
pool=Pool(num_processes)
func=partial(parallelized_function,entity2id_dict,relation2id_dict)
joint_train_ex1=pd.concat(pool.map(func,df_split))
pool.close()
pool.join()


print "Mapping for experiment 1 data valid"
# Mapping the experiment train,valid and test to ID. Will add sameAs links after this.
num_processes=multiprocessing.cpu_count()
# Mappings the train set to ID format for experiment 1!
df_split=np.array_split(joint_valid_ex1,num_processes)
pool=Pool(num_processes)
func=partial(parallelized_function,entity2id_dict,relation2id_dict)
joint_valid_ex1=pd.concat(pool.map(func,df_split))
pool.close()
pool.join()
print "Saving experiment 1 data valid"
joint_valid_ex1.to_csv('temp/valid2id_ex1.txt',header=False,index=False,sep="\t",mode="w+")

print "Mapping for experiment 1 data test"
# Mappings the test set to ID format for experiment 1!
df_split=np.array_split(joint_test_ex1,num_processes)
pool=Pool(num_processes)
func=partial(parallelized_function,entity2id_dict,relation2id_dict)
joint_test_ex1=pd.concat(pool.map(func,df_split))
pool.close()
pool.join()
print "Saving experiment 1 data test"
joint_test_ex1.to_csv('temp/test2id_ex1.txt',header=False,index=False,sep="\t",mode="w+")


spanish_train=spanish_train.iloc[:,0:3]
spanish_valid=spanish_valid.iloc[:,0:3]
spanish_test=spanish_test.iloc[:,0:3]
spanish_test_copy=copy.copy(spanish_test)
spanish_test_copy2=copy.copy(spanish_test)
print "Mapping spanish_test_copy using original entity2id mappings!"
# Mapping spanish_test using original entity2id map for exp2 test
df_split=np.array_split(spanish_test_copy,num_processes)
pool=Pool(num_processes)
func=partial(parallelized_function,entity2id_dict,relation2id_dict)
spanish_test_copy=pd.concat(pool.map(func,df_split))
pool.close()
pool.join()
print "Saving spanish_test with original mappings"
spanish_test_copy.to_csv('temp/spanish_test_original_mappings.txt',header=False,index=False,sep="\t",mode="w+")


modified_entity2id_dict=copy.copy(entity2id_dict)
# print modified_entity2id_dict


print "Joint Test",joint_train_ex1.iloc[0,:]
print "Incorporating sameAs links!"
# For each triple in sameAs,check if subject and object are in the modified_entity2id.If they are present,do:
# 1. Make their ID's same in modified_entity2id.
# 2. Add the triple in Id format to experiment 1 train data.
# SINCE THERE ARE THREE LANGUAGES THIS LOGIC WILL WORK BUT MIGHT NOT WORK WHEN MORE LANGUAGES ARE THERE!!
print "Adding sameAs links to joint_train_ex1 dataset"
print "SameAs Size",sameAs.shape[0]
# for i in range(sameAs.shape[0]):
# 	if(sameAs.iloc[i,0] in entity2id_dict)&(sameAs.iloc[i,2] in entity2id_dict):
# 		joint_train_ex1.loc[joint_train_ex1.shape[0]]=[entity2id_dict[sameAs.iloc[i,0]],sameAs_relation_id,entity2id_dict[sameAs.iloc[i,2]]]
df_split=np.array_split(sameAs,num_processes)
pool=Pool(num_processes)
func=partial(sameAs_adder,entity2id_dict,relation2id_dict)
sameAsLinksMapped=pd.concat(pool.map(func,df_split))
pool.close()
pool.join()
joint_train_ex1.append(sameAsLinksMapped)
# print joint_train_ex1.shape

print "Saving experiment 1 data train"
joint_train_ex1.to_csv('temp/triple2id_ex1.txt',header=False,index=False,sep="\t",mode="w+")

print "Modifying entity2id_dict"
for i in range(sameAs.shape[0]):
	if i%100000==0:
		print i
	if(sameAs.iloc[i,0] in modified_entity2id_dict)&(sameAs.iloc[i,2] in modified_entity2id_dict):
		minimum_value=min(modified_entity2id_dict[sameAs.iloc[i,0]],modified_entity2id_dict[sameAs.iloc[i,2]])
		modified_entity2id_dict[sameAs.iloc[i,0]]=minimum_value
		modified_entity2id_dict[sameAs.iloc[i,2]]=minimum_value

# print joint_train_ex1
# print modified_entity2id_dict

# Converting modified entity2id dictionary to a data frame so that can be saved
modified_entity2id_df=pd.DataFrame(modified_entity2id_dict.items(),columns=["Entity","Id"])
modified_entity2id_df=modified_entity2id_df.sort_values(by="Id")
print "Saving modified_entity2id_df"
modified_entity2id_df.to_csv('temp/modified_entity2id.txt',header=False,index=False,sep="\t",mode="w+")

print "Mapping for experiment 2 data train"
# Mappings the train set to ID format for experiment 2!
df_split=np.array_split(joint_train_ex2,num_processes)
pool=Pool(num_processes)
func=partial(parallelized_function,modified_entity2id_df,relation2id_dict)
joint_train_ex2=pd.concat(pool.map(func,df_split))
pool.close()
pool.join()
print "Saving experiment 2 data train"
joint_train_ex2.to_csv('temp/triple2id_ex2.txt',header=False,index=False,sep="\t",mode="w+")


print "Mapping for experiment 2 data validation"
# Mappings the validaiton set to ID format for experiment 2!
df_split=np.array_split(joint_valid_ex2,num_processes)
pool=Pool(num_processes)
func=partial(parallelized_function,modified_entity2id_df,relation2id_dict)
joint_valid_ex2=pd.concat(pool.map(func,df_split))
pool.close()
pool.join()
print "Saving experiment 2 data valid"
joint_valid_ex2.to_csv('temp/valid2id_ex2.txt',header=False,index=False,sep="\t",mode="w+")


print "Mapping for experiment 2 data test"
# Mappings the test set to ID format for experiment 2!
df_split=np.array_split(joint_test_ex2,num_processes)
pool=Pool(num_processes)
func=partial(parallelized_function,modified_entity2id_df,relation2id_dict)
joint_test_ex2=pd.concat(pool.map(func,df_split))
pool.close()
pool.join()
print "Saving experiment 2 data train"
joint_test_ex2.to_csv('temp/test2id_ex2.txt',header=False,index=False,sep="\t",mode="w+")

print "Mapping spanish_test_copy2 using new entity2id mappings!"
# Mapping spanish_test using new entity2id map for exp2 test
df_split=np.array_split(spanish_test_copy2,num_processes)
pool=Pool(num_processes)
func=partial(parallelized_function,modified_entity2id_dict,relation2id_dict)
spanish_test_copy2=pd.concat(pool.map(func,df_split))
pool.close()
pool.join()
print "Saving spanish_test with new mappings"
spanish_test_copy2.to_csv('temp/spanish_test_new_mappings.txt',header=False,index=False,sep="\t",mode="w+")


# Mapping the spanish train and test datasets
# First read in only spanish mappings! 
# Reading in the entity2id files
print "Reading spanish_entity2id"
all_files=listdir('temp/Spanish_Entity2id/')
only_bz2=[x for x in all_files if x.endswith('bz2')]

spanish_entity2id=pd.DataFrame()
for filename in only_bz2:
	df=pd.read_table('temp/Spanish_Entity2id/'+filename,sep="\t",header=None,compression="bz2",names=["Entity","Id"])
	if df.empty==False:
		spanish_entity2id=spanish_entity2id.append(df)

spanish_entity2id=pd.read_csv('temp/spanish_entity2id.txt',sep="\t",header=None,names=["Entity","Id"])

spanish_entity2id=spanish_entity2id.sort_values(by="Id").reset_index(drop=True)
# Converting to a dictionary
spanish_entity2id_dict=spanish_entity2id.set_index("Entity")["Id"].to_dict()

print "Reading spanish_relation2id"
# Reading in the relation2id files
all_files=listdir('temp/Spanish_Relation2id/')
only_bz2=[x for x in all_files if x.endswith('bz2')]

spanish_relation2id=pd.DataFrame()
for filename in only_bz2:
	df=pd.read_table('temp/Spanish_Relation2id/'+filename,sep="\t",header=None,compression="bz2",names=["Relation","Id"])
	if df.empty==False:
		spanish_relation2id=spanish_relation2id.append(df)

spanish_realtion2id=pd.read_csv('temp/spanish_relation2id.txt',sep="\t",header=None,names=["Relation","Id"])

spanish_relation2id=spanish_relation2id.sort_values(by="Id").reset_index(drop=True)
# Converting to a dictionary
spanish_relation2id_dict=spanish_relation2id.set_index("Relation")["Id"].to_dict()

print "Saving spanish mappings"
spanish_entity2id.to_csv('temp/spanish_entity2id.txt',header=False,index=False,sep="\t",mode="w+")
spanish_relation2id.to_csv('temp/spanish_relation2id.txt',header=False,index=False,sep="\t",mode="w+")

spanish_train=pd.read_csv('temp/Spanish_Train_URI.txt',sep="\t",header=None,names=["Subject","Predicate","Object"])
print "Mapping only spanish train"
# Mappings the train set to ID format for only spanish!
df_split=np.array_split(spanish_train,num_processes)
pool=Pool(num_processes)
func=partial(parallelized_function,spanish_entity2id_dict,spanish_relation2id_dict)
spanish_train=pd.concat(pool.map(func,df_split))
pool.close()
pool.join()
print "Saving spanish train"
spanish_train.to_csv('temp/spanish_train.txt',header=False,index=False,sep="\t",mode="w+")

print "Mapping only spanish valid"
# Mappings the valid set to ID format for only spanish!
df_split=np.array_split(spanish_valid,num_processes)
pool=Pool(num_processes)
func=partial(parallelized_function,spanish_entity2id_dict,spanish_relation2id_dict)
spanish_valid=pd.concat(pool.map(func,df_split))
pool.close()
pool.join()
print "Saving spanish validation"
spanish_valid.to_csv('temp/spanish_valid.txt',header=False,index=False,sep="\t",mode="w+")

print "Mapping only spanish test"
# Mappings the test set to ID format for only spanish!
df_split=np.array_split(spanish_test,num_processes)
pool=Pool(num_processes)
func=partial(parallelized_function,spanish_entity2id_dict,spanish_relation2id_dict)
spanish_test=pd.concat(pool.map(func,df_split))
pool.close()
pool.join()
print "Saving spanish test"
spanish_test.to_csv('temp/spanish_test.txt',header=False,index=False,sep="\t",mode="w+")




print "Adding first lines"
Adding the number of lines of triples train file experiment 1
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

# Adding the number of lines of triples spanish train data
f=open('temp/spanish_train.txt','r')
temp=f.read()
f.close()
f=open('temp/spanish_train.txt','w')
f.write(str(spanish_train.shape[0])+"\n"+temp)
f.close()


# Adding number of lines to spanish_entity2id file
f=open('temp/spanish_entity2id.txt','r')
temp=f.read()
f.close()
f=open('temp/spanish_entity2id.txt','w')
f.write(str(spanish_entity2id.shape[0])+"\n"+temp)
f.close()

#  Adding number of lines to relation2id file
f=open('temp/spanish_relation2id.txt','r')
temp=f.read()
f.close()
f=open('temp/spanish_relation2id.txt','w')
f.write(str(spanish_relation2id.shape[0])+"\n"+temp)
f.close()



## HAVE TO DO TWO THINGS
# 1) spanish_80 using original mappings
# 2) When parallelized_function returns,dont overwrite,join it! 