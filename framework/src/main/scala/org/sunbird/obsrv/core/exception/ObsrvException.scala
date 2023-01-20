package org.sunbird.obsrv.core.exception

import org.sunbird.obsrv.core.model.ErrorConstants.Error
class ObsrvException (val error: Error) extends Exception(error.errorMsg) {

}
