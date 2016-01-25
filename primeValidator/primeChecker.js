function primeCheck()
{ 	//var number=document.getElementById("numvalue").value();
 
   var number=document.myform.numvalue.value; 
  // document.write(number+"");
 
 if(number == "")
  {
    alert("please enter a number");
  }
  else if (number < 0 || (IsInteger(number))==false)
  {
    alert("Prime numbers cannot be negative or decimal")
  }


else  if(number<2)
  { 
    alert("not prime");  
  }
  else{
  var flag = true;
        for(var i = 2; i <= Math.sqrt(number); i++) {
		
		
        if(number % i == 0) {
            flag=false;
			break;
		}
		}
		if(flag)
{
alert("prime");
}
else
{
alert("not prime");
		
		
}   


 }  

 function IsInteger(x)
 {
  return x%1===0;
 }
 
 document.getElementById("form1").reset();
}
