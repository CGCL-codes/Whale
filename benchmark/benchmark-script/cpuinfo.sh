
gicalNumber=0
HTNumber=0
 
 logicalNumber=$(grep "processor" /proc/cpuinfo|sort -u|wc -l)
  physicalNumber=$(grep "physical id" /proc/cpuinfo|sort -u|wc -l)
   coreNumber=$(grep "cpu cores" /proc/cpuinfo|uniq|awk -F':' '{print $2}'|xargs)
    HTNumber=$((logicalNumber / (physicalNumber * coreNumber)))
		  
		  echo "****** CPU Information ******"
		    echo "Logical CPU Number  : ${logicalNumber}"
			  echo "Physical CPU Number : ${physicalNumber}"
			    echo "CPU Core Number     : ${coreNumber}"
				  echo "HT Number           : ${HTNumber}"
				     
				     echo "*****************************"

