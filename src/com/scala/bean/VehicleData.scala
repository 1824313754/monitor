package bean

import scala.beans.BeanProperty

class VehicleData extends Serializable {
  @BeanProperty var vehicleFactory: String = _
  @BeanProperty var vin: String = _
  @BeanProperty var ctime: String = _
  @BeanProperty var ctimeTimeStemp: Long = _
  @BeanProperty var nowTime:String=_
  @BeanProperty var sourceType: String = _
}
